#[macro_export]
macro_rules! db_matrix_test {
    ($name:ident, |$setup:ident| $body:block) => {
        #[test]
        fn $name() {
            for backend in crate::common::configured_backends() {
                let $setup = || crate::common::setup_backend(backend);
                $body
            }
        }
    };
}
