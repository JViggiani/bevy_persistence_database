//! Internal traits and helpers for compile-time checks in derive macros.
//! Not intended for public use.

use bevy::prelude::Component;

#[doc(hidden)]
pub trait IsComponent {
    const VALUE: bool;
}

// The default case: a struct that always resolves to `false`.
#[doc(hidden)]
pub struct IsNotComponent;
impl IsComponent for IsNotComponent {
    const VALUE: bool = false;
}

// The specialized case: a struct that resolves to `true` only if `T` is a `Component`.
#[doc(hidden)]
pub struct IsComponentImpl<T: Component>(std::marker::PhantomData<T>);
impl<T: Component> IsComponent for IsComponentImpl<T> {
    const VALUE: bool = true;
}

// This is the core of the solution. This macro uses a trick with `size_of` on a
// zero-sized array. If the trait bound `<IsComponentImpl<T> as IsComponent>` is
// met, the array has a size of 0. If it is NOT met, the code is invalid, but
// because it's inside the `size_of`, it's a compile-time error that can be
// caught by the `#[cfg]`-like behavior of the array's size check, rather than
// a hard error that stops compilation. The `[(); 0]` is the "false" branch.
#[doc(hidden)]
#[macro_export]
macro_rules! is_component {
    ($T:ty) => {
        (
            [
                $crate::internal_traits::IsNotComponent,
                // This line is the "true" branch. It only compiles if $T is a Component.
                $crate::internal_traits::IsComponentImpl::<$T>,
            ][{
                // This block becomes the index into the array.
                // It evaluates to 1 if the type is a component, 0 otherwise.
                const IMPL_EXISTS: bool = {
                    // A helper trait to check if the specialized impl exists.
                    trait DoesImpl<T> {
                        const VALUE: bool = false;
                    }
                    // This blanket impl is always available.
                    impl<T> DoesImpl<T> for T {}
                    // This struct is used to find the specialized impl.
                    struct Specialized;
                    // This impl is only available if T is a Component.
                    impl<T: $crate::prelude::Component> DoesImpl<T> for Specialized {
                        const VALUE: bool = true;
                    }
                    <Specialized as DoesImpl<$T>>::VALUE
                };
                IMPL_EXISTS as usize
            }]
        ).VALUE
    };
}