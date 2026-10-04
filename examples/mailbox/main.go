// Command mailbox sends end-to-end encrypted messages through a Ricochet
// server to peers that may be offline, and collects them.
//
//	mailbox -key alice.key id
//	mailbox -server <addr> -key alice.key send <peer-id> <text>
//	mailbox -server <addr> -key bob.key inbox
//	mailbox -server <addr> -key bob.key listen
//
// The server address is a multiaddr that ends in /p2p/<server peer ID>, as
// the server prints it. The key file holds this peer's identity and is
// created on first use; keep it, because it is the peer's address.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	udxtransport "github.com/stephanfeb/go-libp2p-udx-transport"

	"github.com/stephanfeb/go-ricochet/pkg/client"
	"github.com/stephanfeb/go-ricochet/pkg/wire"
)

const usage = `Usage: mailbox [flags] <command>

Commands:
  id                      print this peer's ID (no server needed)
  send <peer-id> <text>   store an encrypted message for a peer
  inbox                   print and acknowledge the messages waiting for you
  listen                  stay online, print new messages as they arrive

Flags:
`

func main() {
	flags := flag.NewFlagSet("mailbox", flag.ExitOnError)
	server := flags.String("server", os.Getenv("RICOCHET_SERVER"),
		"server multiaddr ending in /p2p/<peer ID> (default $RICOCHET_SERVER)")
	keyFile := flags.String("key", "mailbox.key", "identity file, created if missing")
	keep := flags.Bool("keep", false, "inbox: leave the messages on the server")
	flags.Usage = func() {
		fmt.Fprint(os.Stderr, usage)
		flags.PrintDefaults()
	}
	_ = flags.Parse(os.Args[1:])
	args := flags.Args()
	if len(args) == 0 {
		flags.Usage()
		os.Exit(2)
	}

	priv, err := loadOrCreateKey(*keyFile)
	if err != nil {
		fail(err)
	}
	if args[0] == "id" {
		id, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			fail(err)
		}
		fmt.Println(id)
		return
	}

	if *server == "" {
		fail(errors.New("-server is required (or set RICOCHET_SERVER)"))
	}
	serverInfo, err := peer.AddrInfoFromString(*server)
	if err != nil {
		fail(fmt.Errorf("server address: %w", err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	h, err := newHost(priv)
	if err != nil {
		fail(err)
	}
	defer h.Close()
	if err := h.Connect(ctx, *serverInfo); err != nil {
		fail(fmt.Errorf("connect to the server: %w", err))
	}

	cl := client.New(h, client.Config{
		PreferredServers: []client.ServerPreference{{PeerID: serverInfo.ID, Priority: 1}},
	})
	defer cl.Close()

	switch args[0] {
	case "send":
		if len(args) < 3 {
			fail(errors.New("usage: send <peer-id> <text>"))
		}
		err = send(ctx, cl, args[1], strings.Join(args[2:], " "))
	case "inbox":
		_, err = inbox(ctx, cl, *keep)
	case "listen":
		err = listen(ctx, h, cl, *serverInfo)
	default:
		flags.Usage()
		os.Exit(2)
	}
	if err != nil {
		fail(err)
	}
}

// newHost creates a libp2p host that speaks what a Ricochet server speaks:
// UDX, Noise and Yamux. The host only dials out, so it works behind NAT:
// the server pushes notifications over the connection the client opened.
func newHost(priv crypto.PrivKey) (host.Host, error) {
	return libp2p.New(
		libp2p.Identity(priv),
		libp2p.NoTransports,
		libp2p.Transport(udxtransport.NewTransport),
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/udp/0/udx"),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer("/yamux/1.0.0", yamux.DefaultTransport),
	)
}

// send stores an encrypted message for the peer with ID to. Only that peer
// can decrypt it; the server stores ciphertext.
func send(ctx context.Context, cl *client.Client, to, text string) error {
	recipient, err := peer.Decode(to)
	if err != nil {
		return fmt.Errorf("recipient peer ID: %w", err)
	}
	res, err := cl.SendMessage(ctx, recipient, []byte(text), client.WithEncryption())
	if err != nil {
		return err
	}
	if err := res.Err(); err != nil {
		return err
	}
	fmt.Printf("Stored for %s as message %s\n", short(recipient.String()), res.MessageID)
	return nil
}

// inbox prints the messages waiting on the server, decrypted, and
// acknowledges them so that the server deletes them, unless keep is set.
// It returns how many it printed.
func inbox(ctx context.Context, cl *client.Client, keep bool) (int, error) {
	msgs, err := cl.RetrieveMessages(ctx)
	if err != nil {
		return 0, err
	}
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		at := time.UnixMilli(m.CreatedTimestamp).Format("15:04:05")
		fmt.Printf("[%s] %s: %s\n", at, short(m.SenderPeerID), m.Payload)
		ids = append(ids, m.MessageID)
	}
	if len(ids) > 0 && !keep {
		if _, err := cl.MarkDelivered(ctx, ids); err != nil {
			return len(msgs), fmt.Errorf("acknowledge: %w", err)
		}
	}
	return len(msgs), nil
}

// listen prints what is waiting, then stays connected. The server counts a
// connected client as online and pushes a notification when a message
// arrives for it; listen then collects the message.
func listen(ctx context.Context, h host.Host, cl *client.Client, server peer.AddrInfo) error {
	newMail := make(chan struct{}, 1)
	cl.RegisterNotificationHandler(func(n *wire.Notification) {
		select {
		case newMail <- struct{}{}:
		default:
		}
	})

	if _, err := inbox(ctx, cl, false); err != nil {
		return err
	}
	fmt.Printf("Listening as %s. Ctrl-C to stop.\n", h.ID())

	reconnect := time.NewTicker(10 * time.Second)
	defer reconnect.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-newMail:
			if _, err := inbox(ctx, cl, false); err != nil {
				fmt.Fprintln(os.Stderr, "!", err)
			}
		case <-reconnect.C:
			// Stay online: if the connection dropped, open it again and
			// collect anything that arrived meanwhile.
			if h.Network().Connectedness(server.ID) == network.Connected {
				continue
			}
			if err := h.Connect(ctx, server); err != nil {
				fmt.Fprintln(os.Stderr, "! server unreachable:", err)
				continue
			}
			if _, err := inbox(ctx, cl, false); err != nil {
				fmt.Fprintln(os.Stderr, "!", err)
			}
		}
	}
}

// loadOrCreateKey reads an Ed25519 identity from path, or creates one there.
// End-to-end encryption derives its X25519 keys from this identity, so it
// must be Ed25519.
func loadOrCreateKey(path string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		return crypto.UnmarshalPrivateKey(data)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	priv, _, err := crypto.GenerateEd25519Key(nil)
	if err != nil {
		return nil, err
	}
	data, err = crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return nil, err
	}
	return priv, nil
}

// short abbreviates a peer ID for display.
func short(id string) string {
	if len(id) <= 8 {
		return id
	}
	return "…" + id[len(id)-6:]
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "mailbox:", err)
	os.Exit(1)
}
