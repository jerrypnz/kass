use futures::future::{lazy, try_join_all, Future, RemoteHandle};
use futures::task::SpawnExt;

pub trait SpawnFuture {
    fn spawn_future<T, E, F>(&mut self, f: F) -> RemoteHandle<Result<T, E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static;
}

impl<P: SpawnExt> SpawnFuture for P {
    fn spawn_future<T, E, F>(&mut self, f: F) -> RemoteHandle<Result<T, E>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        //TODO Error handling
        self.spawn_with_handle(lazy(|_| f()))
            .expect("Failed to spawn task")
    }
}

pub fn traverse<I, T, R, E, F, FN>(xs: I, f: FN) -> impl Future<Output = Result<Vec<R>, E>>
where
    I: IntoIterator<Item = T>,
    F: Future<Output = Result<R, E>>,
    FN: FnMut(T) -> F,
{
    let futures: Vec<F> = xs.into_iter().map(f).collect();
    try_join_all(futures)
}

#[cfg(test)]
mod tests {

    use super::*;
    use futures::executor::{block_on, ThreadPoolBuilder};
    use std::{thread, time};

    #[test]
    pub fn traverse_successful() {
        let mut pool = ThreadPoolBuilder::new().pool_size(5).create().unwrap();
        let interval = time::Duration::from_millis(100);

        let fut = traverse(0..10, |x| {
            pool.spawn_future(move || {
                println!("{} starts sleeping", x);
                thread::sleep(interval);
                println!("{} wakes up", x);
                Ok::<_, String>(x * 2)
            })
        });

        let res = block_on(fut);
        let expected: Vec<i32> = (0..20).step_by(2).collect();
        assert_eq!(res, Ok(expected));
    }
}
