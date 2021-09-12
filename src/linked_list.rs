use std::fmt::Debug;
use std::ops::Deref;
use std::sync::{Arc, RwLock, Weak};

#[derive(Debug)]
pub(crate) struct LinkedList<T> {
    first: Option<Arc<LinkedListNode<T>>>,
    last: Option<Arc<LinkedListNode<T>>>,
}

#[derive(Debug)]
pub struct LinkedListNode<T> {
    next: RwLock<Option<Arc<LinkedListNode<T>>>>,
    prev: RwLock<Option<Weak<LinkedListNode<T>>>>,
    pub value: T,
}

impl<T> LinkedList<T> {
    pub fn new() -> Self {
        Self { first: None, last: None }
    }

    fn set_node_last(&mut self, node: &Arc<LinkedListNode<T>>) {
        if let Some(last) = &self.last {
            *node.prev.write().unwrap() = Some(Arc::downgrade(last));
            *last.next.write().unwrap() = Some(Arc::clone(&node));
        } else if let None = self.first {
            self.first = Some(Arc::clone(&node));
        }
        self.last = Some(Arc::clone(&node));
    }

    pub fn push(&mut self, value: T) -> Arc<LinkedListNode<T>> {
        let node = Arc::new(LinkedListNode {
            next: RwLock::new(None),
            prev: RwLock::new(None),
            value,
        });
        self.set_node_last(&node);
        node
    }

    pub fn move_to_last(&mut self, node: &Arc<LinkedListNode<T>>) {
        let prev = node.prev.read().unwrap().as_ref().cloned();
        let next = node.next.read().unwrap().as_ref().cloned();
        if let Some(next) = node.next.read().unwrap().deref() {
            *next.prev.write().unwrap() = prev;
        } else {
            self.last = prev.map(|prev| prev.upgrade().unwrap());
        }
        if let Some(prev) = node.prev.read().unwrap().deref() {
            *prev.upgrade().unwrap().next.write().unwrap() = next;
        } else {
            self.first = next;
        }
        *node.next.write().unwrap() = None;
        *node.prev.write().unwrap() = None;
        self.set_node_last(node);
    }

    pub fn remove_first(&mut self) -> Option<Arc<LinkedListNode<T>>> {
        if let Some(first) = self.first.take() {
            if let Some(next) = first.next.read().unwrap().deref() {
                *next.prev.write().unwrap() = None;
            } else { self.last = None; }
            self.first = first.next.read().unwrap().deref().clone();
            Some(first)
        } else { None }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::linked_list::LinkedList;

    #[test]
    fn linked_list_test() {
        let mut list = LinkedList::new();
        let rc1 = list.push(1);
        let rc2 = list.push(2);
        let rc3 = list.push(3);
        let rc4 = list.push(4);
        let rc5 = list.push(5);
        let mut map = HashMap::new();
        map.insert(Arc::as_ptr(&rc1), 1);
        map.insert(Arc::as_ptr(&rc2), 2);
        map.insert(Arc::as_ptr(&rc3), 3);
        map.insert(Arc::as_ptr(&rc4), 4);
        map.insert(Arc::as_ptr(&rc5), 5);
        list.move_to_last(&rc3);
        list.move_to_last(&rc1);
        list.move_to_last(&rc1);
        assert_eq!(list.remove_first().map(|rc| Arc::as_ptr(&rc)), Some(Arc::as_ptr(&rc2)));
        assert_eq!(list.remove_first().map(|rc| Arc::as_ptr(&rc)), Some(Arc::as_ptr(&rc4)));
        assert_eq!(list.remove_first().map(|rc| Arc::as_ptr(&rc)), Some(Arc::as_ptr(&rc5)));
        assert_eq!(list.remove_first().map(|rc| Arc::as_ptr(&rc)), Some(Arc::as_ptr(&rc3)));
        assert_eq!(list.remove_first().map(|rc| Arc::as_ptr(&rc)), Some(Arc::as_ptr(&rc1)));
        assert_eq!(list.remove_first().map(|rc| Arc::as_ptr(&rc)), None);
    }
}
