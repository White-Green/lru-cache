use bimap::{BiHashMap, BiBTreeMap};
use std::hash::Hash;

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
pub trait CacheBiMapBackend<Left> {
    fn new() -> Self;
    fn get_by_left(&mut self, left: &Left) -> Option<usize>;
    fn get_by_right(&mut self, right: usize) -> Option<&Left>;
    fn swap_by_right(&mut self, a: usize, b: usize);
    fn remove_by_right(&mut self, right: usize);
    fn insert(&mut self, left: Left, right: usize);
}

impl<Left: Eq + Hash> CacheBiMapBackend<Left> for BiHashMap<Left, usize> {
    fn new() -> Self {
        BiHashMap::new()
    }

    fn get_by_left(&mut self, left: &Left) -> Option<usize> {
        Self::get_by_left(self, left).copied()
    }

    fn get_by_right(&mut self, right: usize) -> Option<&Left> {
        Self::get_by_right(self, &right)
    }

    fn swap_by_right(&mut self, a: usize, b: usize) {
        let a = Self::remove_by_right(self, &a);
        let b = Self::remove_by_right(self, &b);
        if let (Some(a), Some(b)) = (a, b) {
            Self::insert(self, a.0, b.1);
            Self::insert(self, b.0, a.1);
        } else {
            unreachable!()
        }
    }

    fn remove_by_right(&mut self, right: usize) {
        Self::remove_by_right(self, &right);
    }

    fn insert(&mut self, left: Left, right: usize) {
        Self::insert(self, left, right);
    }
}

impl<Left: Ord> CacheBiMapBackend<Left> for BiBTreeMap<Left, usize> {
    fn new() -> Self {
        BiBTreeMap::new()
    }

    fn get_by_left(&mut self, left: &Left) -> Option<usize> {
        Self::get_by_left(self, left).copied()
    }

    fn get_by_right(&mut self, right: usize) -> Option<&Left> {
        Self::get_by_right(self, &right)
    }

    fn swap_by_right(&mut self, a: usize, b: usize) {
        let a = Self::remove_by_right(self, &a);
        let b = Self::remove_by_right(self, &b);
        if let (Some(a), Some(b)) = (a, b) {
            Self::insert(self, a.0, b.1);
            Self::insert(self, b.0, a.1);
        } else {
            unreachable!()
        }
    }

    fn remove_by_right(&mut self, right: usize) {
        Self::remove_by_right(self, &right);
    }

    fn insert(&mut self, left: Left, right: usize) {
        Self::insert(self, left, right);
    }
}

#[derive(PartialEq, Debug)]
struct CacheItem<Index, Item> {
    accessed_time: usize,
    index: Index,
    item: Item,
    updated: bool,
}

/// LRUキャッシュの実装本体
/// 基本的にはtype定義を利用してください
#[derive(PartialEq, Debug)]
pub struct LRU<Back: CacheBackend, Map: CacheBiMapBackend<Back::Index>> {
    cache: Vec<CacheItem<Back::Index, Back::Item>>,
    backend: Back,
    map: Map,
    current_time: usize,
    weight_sum: usize,
    capacity: usize,
}

/// 内部にBiHashMapを利用するLRUキャッシュ
/// `Back::Index : Eq + Hash` が必要
pub type LRUCache<Back> = LRU<Back, BiHashMap<<Back as CacheBackend>::Index, usize>>;

/// 内部にBiBTreeMapを利用するLRUキャッシュ
/// `Back::Index : Ord` が必要
pub type BTreeLRUCache<Back> = LRU<Back, BiBTreeMap<<Back as CacheBackend>::Index, usize>>;

impl<Back: CacheBackend, Map: CacheBiMapBackend<Back::Index>> LRU<Back, Map> {
    /// with_capacity(backend, 10)
    pub fn new(backend: Back) -> Self {
        Self::with_capacity(backend, 10)
    }

    /// バックエンドと容量制限を設定してキャッシュを作成
    pub fn with_capacity(backend: Back, capacity: usize) -> Self {
        Self {
            cache: Vec::new(),
            backend,
            map: Map::new(),
            current_time: 0,
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

impl<Back: CacheBackend, Map: CacheBiMapBackend<Back::Index>> LRU<Back, Map> {
    fn get_inner(&mut self, index: &Back::Index, update: bool) -> Option<&mut Back::Item> {
        if let Some(i) = self.map.get_by_left(&index) {
            let mut value = self.cache.swap_remove(i);
            self.current_time += 1;
            value.accessed_time = self.current_time;
            value.updated |= update;
            let x = self.map.get_by_right(self.cache.len()).unwrap().clone();
            self.map.insert(x, i);
            self.recursive_swap(i);
            self.map.insert(index.clone(), self.cache.len());
            self.cache.push(value);
            self.cache.last_mut().map(|v| &mut v.item)
        } else if let Some(item) = self.backend.load_from_backend(index) {
            self.insert_cache(index.clone(), item, update);
            self.cache.last_mut().map(|v| &mut v.item)
        } else {
            None
        }
    }

    fn recursive_swap(&mut self, mut current: usize) {
        while current * 2 + 1 < self.cache.len() {
            if self.cache[current].accessed_time < self.cache[current * 2 + 1].accessed_time &&
                (current * 2 + 2 >= self.cache.len() || self.cache[current].accessed_time < self.cache[current * 2 + 2].accessed_time) {
                break;
            } else if current * 2 + 2 >= self.cache.len() || self.cache[current * 2 + 1].accessed_time < self.cache[current * 2 + 2].accessed_time {
                self.map.swap_by_right(current, current * 2 + 1);
                self.cache.swap(current, current * 2 + 1);
                current = current * 2 + 1;
            } else {
                self.map.swap_by_right(current, current * 2 + 2);
                self.cache.swap(current, current * 2 + 2);
                current = current * 2 + 2;
            }
        }
    }

    fn insert_cache(&mut self, index: Back::Index, item: Back::Item, updated: bool) {
        self.current_time += 1;
        let item = CacheItem {
            accessed_time: self.current_time,
            index: index.clone(),
            item,
            updated,
        };
        while self.weight_sum > self.capacity {
            self.weight_sum -= self.unload_newest();
        }
        self.weight_sum += self.backend.get_weight(&index, &item.item);
        self.map.insert(index, self.cache.len());
        self.cache.push(item);
    }

    fn unload_newest(&mut self) -> usize {
        if self.cache.is_empty() { return 0; }
        self.map.swap_by_right(0, self.cache.len() - 1);
        self.map.remove_by_right(self.cache.len() - 1);
        let item = self.cache.swap_remove(0);
        let weight = self.backend.get_weight(&item.index, &item.item);
        self.recursive_swap(0);
        self.backend.write_back(item.index, item.item, item.updated);
        weight
    }
}

#[cfg(test)]
mod tests {
    use super::{CacheBackend, LRUCache};
    use self::Log::{Load, Write};
    use std::collections::VecDeque;

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
        let mut cache = LRUCache::with_capacity(VecDeque::new(), 2);
        cache.insert(0, 0);
        assert_eq!(cache.map.len(), 1);
        cache.insert(1, 1);
        assert_eq!(cache.map.len(), 2);
        cache.insert(2, 2);
        assert_eq!(cache.map.len(), 3);
        cache.insert(3, 3);
        assert_eq!(cache.map.len(), 3);
        assert_eq!(cache.backend.pop_front(), Some(Write(0, true)));
        cache.insert(4, 4);
        assert_eq!(cache.map.len(), 3);
        assert_eq!(cache.backend.pop_front(), Some(Write(1, true)));
        cache.insert(5, 5);
        assert_eq!(cache.map.len(), 3);
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
