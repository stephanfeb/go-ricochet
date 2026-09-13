// Package wire is the message model shared by the Ricochet server and its
// clients: the message envelope and its flags and priorities, mailbox types
// and access modes, the acknowledgements the server returns, the document
// store's responses, push notifications, and the status codes that classify
// a refusal.
//
// It is the one package an application needs besides pkg/client, and it is
// deliberately free of server internals so the client library can be used
// from any module. The server imports it through internal/core, which
// aliases every name here so that server code reads the same as before.
package wire
