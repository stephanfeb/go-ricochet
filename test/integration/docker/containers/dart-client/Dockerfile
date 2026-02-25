# Dart client for store integration tests
# Build context must be the agentic/ root directory to include path dependencies
#
# Dependency graph (path deps only):
#   ricochet-dart-client → dart-libp2p → dart-udx
#                        → dart-libp2p-kad-dht → dart-libp2p
#                        → dart-libp2p-pubsub → dart-libp2p, dart-libp2p-kad-dht, dart-udx
#                        → dart-libp2p-merkle-crdt → merkledag, dart-libp2p-pubsub, dart-libp2p, dart-libp2p-kad-dht
#                        → merkledag (no path deps)

FROM dart:3.6

WORKDIR /app

# Copy ricochet-dart-client as the main project
COPY ricochet-dart-client/ ./

# Copy all sibling path dependencies
COPY dart-libp2p/ ../dart-libp2p/
COPY dart-udx/ ../dart-udx/
COPY dart-libp2p-kad-dht/ ../dart-libp2p-kad-dht/
COPY dart-libp2p-pubsub/ ../dart-libp2p-pubsub/
COPY dart-libp2p-merkle-crdt/ ../dart-libp2p-merkle-crdt/
COPY merkledag/ ../merkledag/

# Rewrite all path dependencies to container-relative paths.
# Remove existing dependency_overrides and append correct ones for ALL packages.
RUN sed -i '/^dependency_overrides:/,$ d' pubspec.yaml && \
    printf '\ndependency_overrides:\n  dart_libp2p:\n    path: ../dart-libp2p\n  dart_udx:\n    path: ../dart-udx\n  dart_libp2p_kad_dht:\n    path: ../dart-libp2p-kad-dht\n  dart_libp2p_pubsub:\n    path: ../dart-libp2p-pubsub\n  dart_libp2p_merkle_crdt:\n    path: ../dart-libp2p-merkle-crdt\n  merkledag:\n    path: ../merkledag\n' >> pubspec.yaml

# Also fix path deps in ricochet-dart-client's own dependencies section
# (replace absolute host paths with container-relative paths)
RUN sed -i 's|path: /Users/[^ ]*/dart-libp2p$|path: ../dart-libp2p|' pubspec.yaml && \
    sed -i 's|path: /Users/[^ ]*/dart-libp2p-kad-dht$|path: ../dart-libp2p-kad-dht|' pubspec.yaml && \
    sed -i 's|path: /Users/[^ ]*/dart-libp2p-pubsub$|path: ../dart-libp2p-pubsub|' pubspec.yaml && \
    sed -i 's|path: /Users/[^ ]*/dart-libp2p-merkle-crdt$|path: ../dart-libp2p-merkle-crdt|' pubspec.yaml && \
    sed -i 's|path: /Users/[^ ]*/merkledag$|path: ../merkledag|' pubspec.yaml

# Fix path deps in dart-libp2p's pubspec.yaml (depends on dart-udx)
RUN sed -i 's|path: .*/dart-udx|path: ../dart-udx|' ../dart-libp2p/pubspec.yaml && \
    sed -i '/^dependency_overrides:/,$ d' ../dart-libp2p/pubspec.yaml && \
    printf '\ndependency_overrides:\n  dart_udx:\n    path: ../dart-udx\n' >> ../dart-libp2p/pubspec.yaml

# Fix path deps in dart-libp2p-kad-dht's pubspec.yaml
RUN sed -i 's|path: .*/dart-libp2p$|path: ../dart-libp2p|' ../dart-libp2p-kad-dht/pubspec.yaml && \
    sed -i 's|path: .*/dart-udx|path: ../dart-udx|' ../dart-libp2p-kad-dht/pubspec.yaml

# Fix path deps in dart-libp2p-pubsub's pubspec.yaml
RUN sed -i 's|path: .*/dart-libp2p$|path: ../dart-libp2p|' ../dart-libp2p-pubsub/pubspec.yaml && \
    sed -i 's|path: .*/dart-libp2p-kad-dht|path: ../dart-libp2p-kad-dht|' ../dart-libp2p-pubsub/pubspec.yaml && \
    sed -i 's|path: .*/dart-udx|path: ../dart-udx|' ../dart-libp2p-pubsub/pubspec.yaml

# Fix path deps in dart-libp2p-merkle-crdt's pubspec.yaml
RUN sed -i 's|path: .*/merkledag|path: ../merkledag|' ../dart-libp2p-merkle-crdt/pubspec.yaml && \
    sed -i 's|path: .*/dart-libp2p-pubsub|path: ../dart-libp2p-pubsub|' ../dart-libp2p-merkle-crdt/pubspec.yaml && \
    sed -i 's|path: .*/dart-libp2p$|path: ../dart-libp2p|' ../dart-libp2p-merkle-crdt/pubspec.yaml && \
    sed -i 's|path: .*/dart-libp2p-kad-dht|path: ../dart-libp2p-kad-dht|' ../dart-libp2p-merkle-crdt/pubspec.yaml

# Install dependencies
RUN dart pub get

# Compile the store interop test binary
RUN dart compile exe bin/store_interop_test.dart -o /app/store_test_binary

# Copy entrypoint script
COPY go-ricochet/test/integration/docker/containers/dart-client/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
