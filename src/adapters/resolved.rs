//! The read adapters of a page, resolved once against its column types.

/// The adapters of a value and below it. Exists only where at least one adapter applies.
pub(crate) struct AdapterTree {
    /// The item; the key and the value; or the fields by position.
    /// `None` where no adapter applies to that child or below it.
    children: Box<[Option<AdapterTree>]>,
}

impl AdapterTree {
    fn child(&self, index: usize) -> Option<&Self> {
        self.children.get(index)?.as_ref()
    }

    /// The element of a list, set or vector.
    pub(crate) fn item(&self) -> Option<&Self> {
        self.child(0)
    }

    pub(crate) fn key(&self) -> Option<&Self> {
        self.child(0)
    }

    pub(crate) fn value(&self) -> Option<&Self> {
        self.child(1)
    }

    /// A tuple element or UDT field, by position.
    pub(crate) fn field(&self, index: usize) -> Option<&Self> {
        self.child(index)
    }
}
