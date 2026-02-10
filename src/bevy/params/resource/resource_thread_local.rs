//! Thread-local overrides for persisted resource operations (store selection, refresh hints).
//! These flags are consumed on first use by `PersistentRes`/`PersistentResMut`.

use std::cell::{Cell, RefCell};

thread_local! {
    static PR_STORE: RefCell<Option<String>> = const { RefCell::new(None) };
    static PR_FORCE_REFRESH: Cell<bool> = const { Cell::new(false) };
}

pub fn set_resource_store(store: impl Into<String>) {
    PR_STORE.with(|s| *s.borrow_mut() = Some(store.into()));
}

pub fn take_resource_store() -> Option<String> {
    PR_STORE.with(|s| s.borrow_mut().take())
}

pub fn set_resource_force_refresh() {
    PR_FORCE_REFRESH.with(|f| f.set(true));
}

pub fn take_resource_force_refresh() -> bool {
    PR_FORCE_REFRESH.with(|f| {
        let cur = f.get();
        f.set(false);
        cur
    })
}
