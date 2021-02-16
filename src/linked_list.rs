use std::cell::RefCell;
use std::fmt::Debug;
use std::ops::Deref;
use std::rc::{Rc, Weak};

#[derive(Debug)]
pub(crate) struct LinkedList<T> {
    first: Option<Rc<LinkedListNode<T>>>,
    last: Option<Rc<LinkedListNode<T>>>,
}

#[derive(Debug)]
pub struct LinkedListNode<T> {
    next: RefCell<Option<Rc<LinkedListNode<T>>>>,
    prev: RefCell<Option<Weak<LinkedListNode<T>>>>,
    pub value: T,
}

impl<T> LinkedList<T> {
    pub fn new() -> Self {
        Self { first: None, last: None }
    }

    fn set_node_last(&mut self, node: &Rc<LinkedListNode<T>>) {
        if let Some(last) = &self.last {
            node.prev.replace(Some(Rc::downgrade(last)));
            last.next.replace(Some(Rc::clone(&node)));
        } else if let None = self.first {
            self.first = Some(Rc::clone(&node));
        }
        self.last = Some(Rc::clone(&node));
    }

    pub fn push(&mut self, value: T) -> Rc<LinkedListNode<T>> {
        let node = Rc::new(LinkedListNode {
            next: RefCell::new(None),
            prev: RefCell::new(None),
            value,
        });
        self.set_node_last(&node);
        node
    }

    pub fn move_to_last(&mut self, node: &Rc<LinkedListNode<T>>) {
        let prev = node.prev.borrow().as_ref().cloned();
        let next = node.next.borrow().as_ref().cloned();
        if let Some(next) = node.next.borrow().deref() {
            next.prev.replace(prev);
        } else {
            self.last = prev.map(|prev| prev.upgrade().unwrap());
        }
        if let Some(prev) = node.prev.borrow().deref() {
            prev.upgrade().unwrap().next.replace(next);
        } else {
            self.first = next;
        }
        node.next.replace(None);
        node.prev.replace(None);
        self.set_node_last(node);
    }

    pub fn remove_first(&mut self) -> Option<Rc<LinkedListNode<T>>> {
        if let Some(first) = self.first.take() {
            if let Some(next) = first.next.borrow().deref() {
                next.prev.replace(None);
            } else { self.last = None; }
            self.first = first.next.borrow().deref().clone();
            Some(first)
        } else { None }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::rc::Rc;

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
        map.insert(Rc::as_ptr(&rc1), 1);
        map.insert(Rc::as_ptr(&rc2), 2);
        map.insert(Rc::as_ptr(&rc3), 3);
        map.insert(Rc::as_ptr(&rc4), 4);
        map.insert(Rc::as_ptr(&rc5), 5);
        list.move_to_last(&rc3);
        list.move_to_last(&rc1);
        list.move_to_last(&rc1);
        assert_eq!(list.remove_first().map(|rc| Rc::as_ptr(&rc)), Some(Rc::as_ptr(&rc2)));
        assert_eq!(list.remove_first().map(|rc| Rc::as_ptr(&rc)), Some(Rc::as_ptr(&rc4)));
        assert_eq!(list.remove_first().map(|rc| Rc::as_ptr(&rc)), Some(Rc::as_ptr(&rc5)));
        assert_eq!(list.remove_first().map(|rc| Rc::as_ptr(&rc)), Some(Rc::as_ptr(&rc3)));
        assert_eq!(list.remove_first().map(|rc| Rc::as_ptr(&rc)), Some(Rc::as_ptr(&rc1)));
        assert_eq!(list.remove_first().map(|rc| Rc::as_ptr(&rc)), None);
    }
}
