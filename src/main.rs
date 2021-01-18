mod arena;

fn foo() {}

fn main() {
    let (s1, r1) = crossbeam::channel::bounded::<i32>(1);
    let (s2, r2) = crossbeam::channel::bounded::<i32>(1);
    crossbeam::select! {
        recv(r1) -> _ => {
            foo();
        },
        recv(r2) -> _ => {
            foo();
            foo();
        }
    }
}
