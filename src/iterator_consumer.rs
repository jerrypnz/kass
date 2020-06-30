use itertools::Itertools;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::JoinHandle;

pub trait IteratorConsumer: Iterator {
    fn consume<E, F>(mut self, n: usize, f: F) -> Result<(), E>
    where
        Self: Send + Sized + 'static,
        Self::Item: Send + 'static,
        E: Send + Sync + Clone + 'static,
        F: Fn(Self::Item) -> Result<(), E> + Send + Sync + 'static,
    {
        assert!(n > 0, "n must be positive");
        let pre_assigned: Vec<Self::Item> = self.by_ref().take(n).collect();
        let queue = Arc::new(Mutex::new(self));
        let error: Arc<RwLock<Option<E>>> = Arc::new(RwLock::new(None));
        let action = Arc::new(f);

        let threads = pre_assigned
            .into_iter()
            .map(|item| spawn_worker(item, action.clone(), queue.clone(), error.clone()))
            .collect_vec();

        for t in threads.into_iter() {
            t.join().unwrap();
        }

        let res = error.read().unwrap();
        res.as_ref().map_or(Ok(()), |x| Err(x.clone()))
    }
}

impl<T: ?Sized> IteratorConsumer for T where T: Iterator {}

fn spawn_worker<I, T, E, F>(
    item: T,
    f: Arc<F>,
    queue: Arc<Mutex<I>>,
    error: Arc<RwLock<Option<E>>>,
) -> JoinHandle<()>
where
    T: Send + 'static,
    I: Iterator<Item = T> + Send + 'static,
    E: Send + Sync + Clone + 'static,
    F: Fn(T) -> Result<(), E> + Send + Sync + 'static,
{
    thread::spawn(move || {
        let mut result = f(item);
        while no_error(&error) {
            match result {
                Ok(_) => {
                    if let Some(item) = next_in_queue(&queue) {
                        result = f(item)
                    } else {
                        break;
                    }
                }
                Err(err) => {
                    set_error(&error, err);
                    break;
                }
            }
        }
    })
}

fn next_in_queue<T>(queue: &Arc<Mutex<impl Iterator<Item = T>>>) -> Option<T> {
    queue.lock().unwrap().next()
}

fn no_error<E>(error: &Arc<RwLock<Option<E>>>) -> bool {
    error.read().unwrap().is_none()
}

fn set_error<E>(error: &Arc<RwLock<Option<E>>>, err: E) {
    let mut err_ref = error.write().unwrap();
    *err_ref = Some(err);
}

#[cfg(test)]
mod tests {
    use super::IteratorConsumer;
    use std::thread;
    use std::time;

    #[test]
    fn test_consume_success() {
        let res: Result<(), &str> = (0..10).consume(3, |x| {
            println!("Consuming {}", x);
            thread::sleep(time::Duration::from_millis(100));
            Ok(())
        });

        assert_eq!(res, Ok(()));
    }

    #[test]
    fn test_consume_failure() {
        let res: Result<(), &str> = (0..10).consume(3, |x| {
            println!("Consuming {}", x);
            thread::sleep(time::Duration::from_millis(100));
            if x == 4 {
                Err("Boom!")
            } else {
                Ok(())
            }
        });

        assert_eq!(res, Err("Boom!"));
    }
}
