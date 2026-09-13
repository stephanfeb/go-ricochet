package wire

// Bounds on the free-text fields a client sends. Each of these is stored
// and most are index keys or echoed in every listing, so an unbounded one
// was a way to store ten megabytes under a short name. A request over a
// bound is refused with StatusBadRequest naming the field. Lengths are in
// bytes of UTF-8.
const (
	// MaxContentTypeLength bounds a document's Content-Type.
	MaxContentTypeLength = 256
	// MaxCollectionNameLength bounds a collection's display name.
	MaxCollectionNameLength = 256
	// MaxCollectionKeyLength bounds a collection item's key.
	MaxCollectionKeyLength = 256
	// MaxFeedTitleLength bounds a feed's title.
	MaxFeedTitleLength = 256
	// MaxFeedDescriptionLength bounds a feed's description.
	MaxFeedDescriptionLength = 4096
	// MaxEntryTypeLength bounds a feed entry's type tag.
	MaxEntryTypeLength = 64
	// MaxDisplayNameLength bounds a directory listing's display name.
	MaxDisplayNameLength = 256
	// MaxBioLength bounds a directory listing's bio.
	MaxBioLength = 4096
	// MaxAvatarHashLength bounds a directory listing's avatar hash.
	MaxAvatarHashLength = 256
	// MaxDirectoryExtrasBytes bounds a directory listing's extras as JSON.
	MaxDirectoryExtrasBytes = 4096
)
