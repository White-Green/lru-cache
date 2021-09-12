use std::collections::{BTreeMap, HashMap, VecDeque};
use std::hash::Hash;

use crate::linked_list::{LinkedList, LinkedListNode};
use std::sync::Arc;

mod linked_list;

/// LRUキャッシュのためのデータストレージ用trait
///
/// たとえば、ファイルIOをこれでラップするなどする
pub trait CacheBackend {
    /// データの識別に使う型
    type Index: Clone;
    /// データ自体の型
    type Item;

    /// 指定したデータ(存在しないならNone)を取り出す
    fn load_from_backend(&mut self, index: &Self::Index) -> Option<Self::Item>;
    /// キャッシュからデータを書き戻す
    /// 新規に追加されたデータまたはキャッシュされている間に変更されたデータの場合は`updated=true`になる
    fn write_back(&mut self, index: Self::Index, item: Self::Item, updated: bool);
    /// キャッシュの容量制限に利用するデータサイズを計算する
    fn get_weight(&mut self, _index: &Self::Index, _item: &Self::Item) -> usize { 1 }
}

/// キャッシュの内部で利用するBiMap用trait　キャッシュの利用側での実装は必要ない
pub trait CacheMapBackend<Key> {
    fn new() -> Self;
    fn get(&mut self, left: &Key) -> Option<&Arc<LinkedListNode<usize>>>;
    fn remove(&mut self, key: &Key) -> Option<Arc<LinkedListNode<usize>>>;
    fn insert(&mut self, key: Key, value: Arc<LinkedListNode<usize>>);
}

impl<Key: Eq + Hash> CacheMapBackend<Key> for HashMap<Key, Arc<LinkedListNode<usize>>> {
    fn new() -> Self {
        HashMap::new()
    }

    fn get(&mut self, left: &Key) -> Option<&Arc<LinkedListNode<usize>>> {
        Self::get(self, left)
    }

    fn remove(&mut self, key: &Key) -> Option<Arc<LinkedListNode<usize>>> {
        Self::remove(self, key)
    }

    fn insert(&mut self, left: Key, right: Arc<LinkedListNode<usize>>) {
        Self::insert(self, left, right);
    }
}

impl<Key: Ord> CacheMapBackend<Key> for BTreeMap<Key, Arc<LinkedListNode<usize>>> {
    fn new() -> Self {
        BTreeMap::new()
    }

    fn get(&mut self, key: &Key) -> Option<&Arc<LinkedListNode<usize>>> {
        Self::get(self, key)
    }

    fn remove(&mut self, key: &Key) -> Option<Arc<LinkedListNode<usize>>> {
        Self::remove(self, key)
    }

    fn insert(&mut self, key: Key, value: Arc<LinkedListNode<usize>>) {
        Self::insert(self, key, value);
    }
}

#[derive(PartialEq, Debug)]
struct CacheItem<Index, Item> {
    index: Index,
    item: Item,
    updated: bool,
}

/// LRUキャッシュの実装本体
/// 基本的にはtype定義を利用してください
#[derive(Debug)]
pub struct LRU<Back: CacheBackend, Map: CacheMapBackend<Back::Index>> {
    cache: Vec<Option<CacheItem<Back::Index, Back::Item>>>,
    spaces: VecDeque<usize>,
    list: LinkedList<usize>,
    backend: Back,
    map: Map,
    weight_sum: usize,
    capacity: usize,
}

/// 内部にBiHashMapを利用するLRUキャッシュ
/// `Back::Index : Eq + Hash` が必要
pub type LRUCache<Back> = LRU<Back, HashMap<<Back as CacheBackend>::Index, Arc<LinkedListNode<usize>>>>;

/// 内部にBiBTreeMapを利用するLRUキャッシュ
/// `Back::Index : Ord` が必要
pub type BTreeLRUCache<Back> = LRU<Back, BTreeMap<<Back as CacheBackend>::Index, Arc<LinkedListNode<usize>>>>;

impl<Back: CacheBackend, Map: CacheMapBackend<Back::Index>> LRU<Back, Map> {
    /// with_capacity(backend, 10)
    pub fn new(backend: Back) -> Self {
        Self::with_capacity(backend, 10)
    }

    /// バックエンドと容量制限を設定してキャッシュを作成
    pub fn with_capacity(backend: Back, capacity: usize) -> Self {
        Self {
            cache: Vec::new(),
            spaces: VecDeque::new(),
            list: LinkedList::new(),
            backend,
            map: Map::new(),
            weight_sum: 0,
            capacity,
        }
    }

    /// キャッシュからデータを取得する
    pub fn get(&mut self, index: &Back::Index) -> Option<&Back::Item> {
        self.get_inner(index, false).map(|v| v as &Back::Item)
    }

    /// キャッシュからデータを変更可能で取得する
    pub fn get_mut(&mut self, index: &Back::Index) -> Option<&mut Back::Item> {
        self.get_inner(index, true)
    }

    /// キャッシュにデータを追加する
    /// 追加したデータはライトバック方式でバックエンドに書き込まれる
    pub fn insert(&mut self, index: Back::Index, item: Back::Item) {
        self.insert_cache(index, item, true)
    }

    /// バックエンドのオブジェクトを取得する
    pub fn get_backend(&self) -> &Back {
        &self.backend
    }

    /// バックエンドのオブジェクトを取得する
    pub fn get_backend_mut(&mut self) -> &mut Back {
        &mut self.backend
    }
}

impl<Back: CacheBackend, Map: CacheMapBackend<Back::Index>> LRU<Back, Map> {
    fn get_inner(&mut self, index: &Back::Index, update: bool) -> Option<&mut Back::Item> {
        if let Some(i) = self.map.get(&index) {
            // println!("{:#?}", self.list);
            self.list.move_to_last(i);
            // println!("{:#?}", self.list);
            self.cache.get_mut(i.value).map(Option::as_mut).flatten().map(|v| {
                v.updated |= update;
                &mut v.item
            })
        } else if let Some(item) = self.backend.load_from_backend(index) {
            self.insert_cache(index.clone(), item, update);
            self.cache.get_mut(self.map.get(&index).unwrap().value).map(Option::as_mut).flatten().map(|v| &mut v.item)
        } else {
            None
        }
    }

    fn insert_cache(&mut self, index: Back::Index, item: Back::Item, updated: bool) {
        let item = CacheItem {
            index: index.clone(),
            item,
            updated,
        };
        let weight = self.backend.get_weight(&index, &item.item);
        while self.cache.len() > 0 && self.weight_sum + weight > self.capacity {
            self.weight_sum -= self.unload_newest();
        }
        self.weight_sum += weight;
        let (cache_index, space) = if let Some(space) = self.spaces.pop_front() {
            (space, self.cache.get_mut(space).unwrap())
        } else {
            let len = self.cache.len();
            self.cache.push(None);
            (len, self.cache.last_mut().unwrap())
        };
        self.map.insert(index, self.list.push(cache_index));
        space.replace(item);
    }

    fn unload_newest(&mut self) -> usize {
        if let Some(oldest) = self.list.remove_first() {
            let item = self.cache.get_mut(oldest.value).unwrap().take().unwrap();
            self.spaces.push_back(self.map.remove(&item.index).unwrap().value);
            let weight = self.backend.get_weight(&item.index, &item.item);
            self.backend.write_back(item.index, item.item, item.updated);
            weight
        } else { 0 }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::{CacheBackend, LRUCache};

    use self::Log::{Load, Write};

    #[derive(PartialEq, Debug)]
    enum Log {
        Load(usize),
        Write(usize, bool),
    }

    impl CacheBackend for VecDeque<Log> {
        type Index = usize;
        type Item = usize;

        fn load_from_backend(&mut self, index: &Self::Index) -> Option<Self::Item> {
            self.push_back(Load(*index));
            Some(*index)
        }

        fn write_back(&mut self, index: Self::Index, _item: Self::Item, updated: bool) {
            self.push_back(Write(index, updated));
        }
    }

    #[test]
    fn check_algorithm() {
        let mut cache = LRUCache::with_capacity(VecDeque::new(), 3);
        cache.insert(0, 0);
        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3);
        assert_eq!(cache.backend.pop_front(), Some(Write(0, true)));
        cache.insert(4, 4);
        assert_eq!(cache.backend.pop_front(), Some(Write(1, true)));
        cache.insert(5, 5);
        assert_eq!(cache.backend.pop_front(), Some(Write(2, true)));
        assert_eq!(cache.get(&0), Some(&0));
        assert_eq!(cache.backend.pop_front(), Some(Load(0)));
        assert_eq!(cache.backend.pop_front(), Some(Write(3, true)));
        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.backend.pop_front(), Some(Load(1)));
        assert_eq!(cache.backend.pop_front(), Some(Write(4, true)));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.backend.pop_front(), Some(Load(2)));
        assert_eq!(cache.backend.pop_front(), Some(Write(5, true)));
        assert_eq!(cache.get_mut(&3), Some(&mut 3));
        assert_eq!(cache.backend.pop_front(), Some(Load(3)));
        assert_eq!(cache.backend.pop_front(), Some(Write(0, false)));
        assert_eq!(cache.get_mut(&4), Some(&mut 4));
        assert_eq!(cache.backend.pop_front(), Some(Load(4)));
        assert_eq!(cache.backend.pop_front(), Some(Write(1, false)));
        assert_eq!(cache.get_mut(&5), Some(&mut 5));
        assert_eq!(cache.backend.pop_front(), Some(Load(5)));
        assert_eq!(cache.backend.pop_front(), Some(Write(2, false)));
        assert_eq!(cache.get(&0), Some(&0));
        assert_eq!(cache.backend.pop_front(), Some(Load(0)));
        assert_eq!(cache.backend.pop_front(), Some(Write(3, true)));
        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.backend.pop_front(), Some(Load(1)));
        assert_eq!(cache.backend.pop_front(), Some(Write(4, true)));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.backend.pop_front(), Some(Load(2)));
        assert_eq!(cache.backend.pop_front(), Some(Write(5, true)));
        assert_eq!(cache.get_mut(&0), Some(&mut 0));
        assert_eq!(cache.get_mut(&1), Some(&mut 1));
        assert_eq!(cache.get_mut(&2), Some(&mut 2));
        assert_eq!(cache.get(&0), Some(&0));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.get(&0), Some(&0));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&0), Some(&0));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&0), Some(&0));
        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.get(&2), Some(&2));
        assert_eq!(cache.get(&1), Some(&1));
        assert_eq!(cache.get(&0), Some(&0));
        assert_eq!(cache.get(&3), Some(&3));
        assert_eq!(cache.backend.pop_front(), Some(Load(3)));
        assert_eq!(cache.backend.pop_front(), Some(Write(2, true)));
        assert_eq!(cache.get(&4), Some(&4));
        assert_eq!(cache.backend.pop_front(), Some(Load(4)));
        assert_eq!(cache.backend.pop_front(), Some(Write(1, true)));
        assert_eq!(cache.get(&5), Some(&5));
        assert_eq!(cache.backend.pop_front(), Some(Load(5)));
        assert_eq!(cache.backend.pop_front(), Some(Write(0, true)));
    }
}
