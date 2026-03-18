// Package buntdb implements a low-level in-memory key/value store in pure Go.
// 包 `buntdb` 使用纯 Go 实现低层的内存键/值存储。
// It persists to disk, is ACID compliant, and uses locking for multiple
// 它会持久化到磁盘，满足 ACID，并使用锁来支持多个
// readers and a single writer. Bunt is ideal for projects that need a
// 读者与单个写者。Buntdb 特别适合需要可靠数据库、
// dependable database, and favor speed over data size.
// 并在速度与数据体积之间更偏向速度的项目。
package buntdb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
	"github.com/tidwall/gjson"
	"github.com/tidwall/grect"
	"github.com/tidwall/match"
	"github.com/tidwall/rtred"
)

var (
	// ErrTxNotWritable is returned when performing a write operation on a
	// read-only transaction.
	// 当在只读事务上执行写操作时返回该错误。
	// read-only transaction（只读事务）
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed is returned when committing or rolling back a transaction
	// that has already been committed or rolled back.
	// 在提交或回滚一个已经提交/回滚过的事务时返回该错误。
	// that has already been committed or rolled back（已被提交或回滚过）
	ErrTxClosed = errors.New("tx closed")

	// ErrNotFound is returned when an item or index is not in the database.
	// 当数据项或索引不在数据库中时返回该错误。
	ErrNotFound = errors.New("not found")

	// ErrInvalid is returned when the database file is an invalid format.
	// 当数据库文件格式无效时返回该错误。
	ErrInvalid = errors.New("invalid database")

	// ErrDatabaseClosed is returned when the database is closed.
	// 当数据库已关闭时返回该错误。
	ErrDatabaseClosed = errors.New("database closed")

	// ErrIndexExists is returned when an index already exists in the database.
	// 当要创建的索引已存在于数据库时返回该错误。
	ErrIndexExists = errors.New("index exists")

	// ErrInvalidOperation is returned when an operation cannot be completed.
	// 当某个操作无法完成时返回该错误。
	ErrInvalidOperation = errors.New("invalid operation")

	// ErrInvalidSyncPolicy is returned for an invalid SyncPolicy value.
	// 当 SyncPolicy 值无效时返回该错误。
	ErrInvalidSyncPolicy = errors.New("invalid sync policy")

	// ErrShrinkInProcess is returned when a shrink operation is in-process.
	// 当缩容（shrink）正在进行中时返回该错误。
	ErrShrinkInProcess = errors.New("shrink is in-process")

	// ErrPersistenceActive is returned when post-loading data from an database
	// not opened with Open(":memory:").
	// 当从未使用 `Open(":memory:")` 打开的数据库中进行加载后置数据处理时返回该错误。
	// not opened with Open(":memory:").（未以内存模式打开）
	ErrPersistenceActive = errors.New("persistence active")

	// ErrTxIterating is returned when Set or Delete are called while iterating.
	// 当在遍历（iterating）过程中调用 `Set` 或 `Delete` 时返回该错误。
	ErrTxIterating = errors.New("tx is iterating")
)

const useAbsEx = true

// DB represents a collection of key-value pairs that persist on disk.
// DB 表示一组在磁盘上持久化的键值对集合。
// Transactions are used for all forms of data access to the DB.
// 对数据库的所有数据访问都通过事务完成。
type DB struct {
	mu        sync.RWMutex      // the gatekeeper for all fields
	// 所有字段的“门卫”（用于并发访问控制）。
	file      *os.File          // the underlying file
	// 底层文件句柄。
	buf       []byte            // a buffer to write to
	// 写入用缓冲区。
	keys      *btree.BTree      // a tree of all item ordered by key
	// 按 key 有序的全部数据项 B-tree。
	exps      *btree.BTree      // a tree of items ordered by expiration
	// 按过期时间排序的数据项 B-tree。
	idxs      map[string]*index // the index trees.
	// 索引树集合。
	insIdxs   []*index          // a reuse buffer for gathering indexes
	// 复用的缓冲区，用于收集需要更新的索引。
	flushes   int               // a count of the number of disk flushes
	// 磁盘刷新次数统计。
	closed    bool              // set when the database has been closed
	// 数据库关闭时置为 true。
	config    Config            // the database configuration
	// 数据库配置。
	persist   bool              // do we write to disk
	// 是否写入磁盘持久化。
	shrinking bool              // when an aof shrink is in-process.
	// aof 缩容正在进行中时为 true。
	lastaofsz int               // the size of the last shrink aof size
	// 上一次缩容后的 aof 文件大小。
}

// SyncPolicy represents how often data is synced to disk.
// SyncPolicy 用于描述数据同步到磁盘的频率。
type SyncPolicy int

const (
	// Never is used to disable syncing data to disk.
	// Never 用于禁用向磁盘的同步。
	// The faster and less safe method.
	// 更快但不安全的方式。
	Never SyncPolicy = 0
	// EverySecond is used to sync data to disk every second.
	// EverySecond 用于每秒同步到磁盘。
	// It's pretty fast and you can lose 1 second of data if there
	// 性能不错，但如果发生灾难，可能会丢失 1 秒的数据（以下依赖下一行继续）。
	// is a disaster.
	// 如果发生灾难。
	// This is the recommended setting.
	// 推荐使用该设置。
	EverySecond = 1
	// Always is used to sync data after every write to disk.
	// Always 用于每次写入磁盘后都进行同步。
	// Slow. Very safe.
	// 较慢，但非常安全。
	Always = 2
)

// Config represents database configuration options. These
// Config 表示数据库的配置项。这些
// options are used to change various behaviors of the database.
// 配置用于改变数据库的各种行为。
type Config struct {
	// SyncPolicy adjusts how often the data is synced to disk.
	// SyncPolicy 用于调整数据同步到磁盘的频率。
	// This value can be Never, EverySecond, or Always.
	// 该值只能是 Never、EverySecond 或 Always。
	// The default is EverySecond.
	// 默认值为 EverySecond。
	SyncPolicy SyncPolicy

	// AutoShrinkPercentage is used by the background process to trigger
	// a shrink of the aof file when the size of the file is larger than the
	// percentage of the result of the previous shrunk file.
	// 后台进程使用该值：当 aof 文件大小超过上一次缩容后文件大小的百分比时触发缩容。
	// For example, if this value is 100, and the last shrink process
	// resulted in a 100mb file, then the new aof file must be 200mb before
	// a shrink is triggered.
	// 例如：如果该值为 100，上一次缩容得到 100MB 文件，那么当新的 aof 达到 200MB 之前不会触发缩容。
	AutoShrinkPercentage int

	// AutoShrinkMinSize defines the minimum size of the aof file before
	// an automatic shrink can occur.
	// AutoShrinkMinSize 定义：在 aof 达到最小大小之前，不会自动触发缩容。
	AutoShrinkMinSize int

	// AutoShrinkDisabled turns off automatic background shrinking
	// AutoShrinkDisabled 用于关闭自动后台缩容。
	AutoShrinkDisabled bool

	// OnExpired is used to custom handle the deletion option when a key
	// has been expired.
	// 当 key 已过期时，OnExpired 用于自定义删除处理逻辑。
	OnExpired func(keys []string)

	// OnExpiredSync will be called inside the same transaction that is
	// performing the deletion of expired items. If OnExpired is present then
	// this callback will not be called. If this callback is present, then the
	// deletion of the timeed-out item is the explicit responsibility of this
	// callback.
	// OnExpiredSync 会在执行过期项删除的同一个事务内调用。
	// 如果提供了 OnExpired，则不会调用本回调。
	// 如果提供了本回调，则由它显式负责执行超时项的删除。
	OnExpiredSync func(key, value string, tx *Tx) error
}

// exctx is a simple b-tree context for ordering by expiration.
// exctx 是一个用于按过期时间排序的简单 b-tree 上下文。
type exctx struct {
	db *DB
}

// Open opens a database at the provided path.
// Open 在指定路径打开一个数据库。
// If the file does not exist then it will be created automatically.
// 如果文件不存在，将自动创建。
func Open(path string) (*DB, error) {
	db := &DB{}
	// initialize trees and indexes
	// 初始化树与索引。
	db.keys = btreeNew(lessCtx(nil))
	db.exps = btreeNew(lessCtx(&exctx{db}))
	db.idxs = make(map[string]*index)
	// initialize default configuration
	// 初始化默认配置。
	db.config = Config{
		SyncPolicy:           EverySecond,
		AutoShrinkPercentage: 100,
		AutoShrinkMinSize:    32 * 1024 * 1024,
	}
	// turn off persistence for pure in-memory
	// 纯内存模式下关闭持久化。
	db.persist = path != ":memory:"
	if db.persist {
		var err error
		// hardcoding 0666 as the default mode.
		// 将 0666 作为默认文件权限（硬编码）。
		db.file, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return nil, err
		}
		// load the database from disk
		// 从磁盘加载数据库。
		if err := db.load(); err != nil {
			// close on error, ignore close error
			// 发生错误时关闭文件；忽略关闭错误。
			_ = db.file.Close()
			return nil, err
		}
	}
	// start the background manager.
	// 启动后台管理器。
	go db.backgroundManager()
	return db, nil
}

// Close releases all database resources.
// Close 释放所有数据库资源。
// All transactions must be closed before closing the database.
// 在关闭数据库前，必须先结束所有事务。
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	db.closed = true
	if db.persist {
		db.file.Sync() // do a sync but ignore the error
		// 执行一次同步，但忽略同步可能的错误。
		if err := db.file.Close(); err != nil {
			return err
		}
	}
	// Let's release all references to nil. This will help both with debugging
	// late usage panics and it provides a hint to the garbage collector
	// 我们把所有引用置为 nil。这样既能帮助调试“迟到使用”导致的 panic，
	// 也能向垃圾回收器提供提示。
	db.keys, db.exps, db.idxs, db.file = nil, nil, nil, nil
	return nil
}

// Save writes a snapshot of the database to a writer. This operation blocks all
// writes, but not reads. This can be used for snapshots and backups for pure
// in-memory databases using the ":memory:". Database that persist to disk
// can be snapshotted by simply copying the database file.
// Save 会把数据库快照写入到指定 writer。
// 该操作会阻塞所有写入，但不会阻塞读取；适用于路径为 `:memory:` 的纯内存数据库的快照和备份。
// 对会持久化到磁盘的数据库，只需复制数据库文件即可得到快照。
func (db *DB) Save(wr io.Writer) error {
	var err error
	db.mu.RLock()
	defer db.mu.RUnlock()
	// use a buffered writer and flush every 4MB
	// 使用缓冲写入，并在达到 4MB 时刷新。
	var buf []byte
	now := time.Now()
	// iterated through every item in the database and write to the buffer
	// 遍历数据库中的每个数据项，并写入缓冲区。
	btreeAscend(db.keys, func(item interface{}) bool {
		dbi := item.(*dbItem)
		buf = dbi.writeSetTo(buf, now)
		if len(buf) > 1024*1024*4 {
			// flush when buffer is over 4MB
			// 当缓冲区超过 4MB 时刷新。
			_, err = wr.Write(buf)
			if err != nil {
				return false
			}
			buf = buf[:0]
		}
		return true
	})
	if err != nil {
		return err
	}
	// one final flush
	// 最后再刷新一次。
	if len(buf) > 0 {
		_, err = wr.Write(buf)
		if err != nil {
			return err
		}
	}
	return nil
}

// Load loads commands from reader. This operation blocks all reads and writes.
// Note that this can only work for fully in-memory databases opened with
// Open(":memory:").
// Load 从 reader 读取命令并加载到数据库。该操作会阻塞所有读写。
// 注意：这只能用于使用 `Open(":memory:")` 打开的纯内存数据库。
func (db *DB) Load(rd io.Reader) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.persist {
		// cannot load into databases that persist to disk
		// 不能把数据加载到会持久化到磁盘的数据库。
		return ErrPersistenceActive
	}
	_, err := db.readLoad(rd, time.Now())
	return err
}

// index represents a b-tree or r-tree index and also acts as the
// b-tree/r-tree context for itself.
// index 表示一个 b-tree 或 r-tree 索引，并且同时作为其自身的 b-tree/r-tree 上下文。
type index struct {
	btr     *btree.BTree                           // contains the items
	// 包含数据项。
	rtr     *rtred.RTree                           // contains the items
	// 包含数据项。
	name    string                                 // name of the index
	// 索引名称。
	pattern string                                 // a required key pattern
	// 必须匹配的 key 模式。
	less    func(a, b string) bool                 // less comparison function
	// less 比较函数。
	rect    func(item string) (min, max []float64) // rect from string function
	// 将字符串转换为矩形的函数。
	db      *DB                                    // the origin database
	// 原始数据库。
	opts    IndexOptions                           // index options
	// 索引选项。
}

// match matches the pattern to the key
// match 用于将给定 pattern 与 key 进行匹配。
func (idx *index) match(key string) bool {
	if idx.pattern == "*" {
		return true
	}
	if idx.opts.CaseInsensitiveKeyMatching {
		for i := 0; i < len(key); i++ {
			if key[i] >= 'A' && key[i] <= 'Z' {
				key = strings.ToLower(key)
				break
			}
		}
	}
	return match.Match(key, idx.pattern)
}

// clearCopy creates a copy of the index, but with an empty dataset.
// clearCopy 创建索引副本，但数据集为空。
func (idx *index) clearCopy() *index {
	// copy the index meta information
	// 复制索引的元信息。
	nidx := &index{
		name:    idx.name,
		pattern: idx.pattern,
		db:      idx.db,
		less:    idx.less,
		rect:    idx.rect,
		opts:    idx.opts,
	}
	// initialize with empty trees
	// 使用空树初始化。
	if nidx.less != nil {
		nidx.btr = btreeNew(lessCtx(nidx))
	}
	if nidx.rect != nil {
		nidx.rtr = rtred.New(nidx)
	}
	return nidx
}

// rebuild rebuilds the index
// rebuild 重建索引。
func (idx *index) rebuild() {
	// initialize trees
	// 初始化树。
	if idx.less != nil {
		idx.btr = btreeNew(lessCtx(idx))
	}
	if idx.rect != nil {
		idx.rtr = rtred.New(idx)
	}
	// iterate through all keys and fill the index
	// 遍历所有 key 并填充索引。
	btreeAscend(idx.db.keys, func(item interface{}) bool {
		dbi := item.(*dbItem)
		if !idx.match(dbi.key) {
			// does not match the pattern, continue
			// 不匹配 pattern，继续下一个。
			return true
		}
		if idx.less != nil {
			idx.btr.Set(dbi)
		}
		if idx.rect != nil {
			idx.rtr.Insert(dbi)
		}
		return true
	})
}

// CreateIndex builds a new index and populates it with items.
// CreateIndex 创建一个新索引，并用数据项填充它。
// The items are ordered in an b-tree and can be retrieved using the
// 数据项按 b-tree 排序，可通过下面方法获取：
// Ascend* and Descend* methods.
// 使用 `Ascend*` 与 `Descend*`。
// An error will occur if an index with the same name already exists.
// 如果已存在同名索引，将返回错误。
//
// When a pattern is provided, the index will be populated with
// 当提供 pattern 时，索引会被填充为：
// keys that match the specified pattern. This is a very simple pattern
// 所有匹配指定 pattern 的 key。pattern 匹配规则非常简单：
// match where '*' matches on any number characters and '?' matches on
// `*` 匹配任意数量字符，`?` 匹配任意单个字符。
// any one character.
// （以上就是全部规则。）
// The less function compares if string 'a' is less than string 'b'.
// less 函数用于比较：字符串 `a` 是否小于字符串 `b`。
// It allows for indexes to create custom ordering. It's possible
// 它允许索引创建自定义排序方式。
// that the strings may be textual or binary. It's up to the provided
// 字符串可能是文本也可能是二进制；具体如何处理与比较，
// less function to handle the content format and comparison.
// 由你提供的 less 函数决定。
// There are some default less function that can be used such as
// 也提供了一些默认 less 函数，例如：
// IndexString, IndexBinary, etc.
// `IndexString`、`IndexBinary` 等。
func (db *DB) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateIndex(name, pattern, less...)
	})
}

// ReplaceIndex builds a new index and populates it with items.
// ReplaceIndex 创建一个新索引并用数据项填充。
// The items are ordered in an b-tree and can be retrieved using the
// 数据项按 b-tree 排序，并可通过：
// Ascend* and Descend* methods.
// `Ascend*` 与 `Descend*` 方法获取。
// If a previous index with the same name exists, that index will be deleted.
// 如果先前已存在同名索引，则会先删除旧索引。
func (db *DB) ReplaceIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateIndex(name, pattern, less...)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateIndex(name, pattern, less...)
			}
			return err
		}
		return nil
	})
}

// CreateSpatialIndex builds a new index and populates it with items.
// CreateSpatialIndex 创建一个新的空间索引并填充数据项。
// The items are organized in an r-tree and can be retrieved using the
// 数据项会组织到 r-tree 中，并可通过：
// Intersects method.
// `Intersects` 方法查询相交项。
// An error will occur if an index with the same name already exists.
// 如果已存在同名索引，将返回错误。
//
// The rect function converts a string to a rectangle. The rectangle is
// rect 函数用于把字符串转换为矩形（rectangle）。
// represented by two arrays, min and max. Both arrays may have a length
// 矩形由两个数组表示：`min` 与 `max`。
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// 两个数组的长度都必须在 1 到 20 之间，且长度必须一致。
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// 长度为 1 表示一维矩形；长度为 4 表示四维矩形。
// There is support for up to 20 dimensions.
// 支持最多 20 维。
// The values of min must be less than the values of max at the same dimension.
// 在同一维度上，`min` 的值必须小于 `max` 的值。
// Thus min[0] must be less-than-or-equal-to max[0].
// 因此 `min[0]` 必须小于或等于 `max[0]`。
// The IndexRect is a default function that can be used for the rect
// `IndexRect` 是一个默认 rect 函数实现，可直接用于：
// parameter.
// rect 参数。
func (db *DB) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		return tx.CreateSpatialIndex(name, pattern, rect)
	})
}

// ReplaceSpatialIndex builds a new index and populates it with items.
// ReplaceSpatialIndex 创建新的空间索引并填充数据项。
// The items are organized in an r-tree and can be retrieved using the
// 数据项组织到 r-tree 中，并可用：
// Intersects method.
// `Intersects` 查询相交项。
// If a previous index with the same name exists, that index will be deleted.
// 若已存在同名索引，则先删除旧索引。
func (db *DB) ReplaceSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return db.Update(func(tx *Tx) error {
		err := tx.CreateSpatialIndex(name, pattern, rect)
		if err != nil {
			if err == ErrIndexExists {
				err := tx.DropIndex(name)
				if err != nil {
					return err
				}
				return tx.CreateSpatialIndex(name, pattern, rect)
			}
			return err
		}
		return nil
	})
}

// DropIndex removes an index.
// DropIndex 用于删除一个索引。
func (db *DB) DropIndex(name string) error {
	return db.Update(func(tx *Tx) error {
		return tx.DropIndex(name)
	})
}

// Indexes returns a list of index names.
// Indexes 返回索引名称列表。
func (db *DB) Indexes() ([]string, error) {
	var names []string
	var err = db.View(func(tx *Tx) error {
		var err error
		names, err = tx.Indexes()
		return err
	})
	return names, err
}

// ReadConfig returns the database configuration.
// ReadConfig 返回数据库配置。
func (db *DB) ReadConfig(config *Config) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	*config = db.config
	return nil
}

// SetConfig updates the database configuration.
// SetConfig 用于更新数据库配置。
func (db *DB) SetConfig(config Config) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrDatabaseClosed
	}
	switch config.SyncPolicy {
	default:
		return ErrInvalidSyncPolicy
	case Never, EverySecond, Always:
	}
	db.config = config
	return nil
}

// insertIntoDatabase performs inserts an item in to the database and updates
// insertIntoDatabase 将项插入数据库并更新所有索引。
// all indexes. If a previous item with the same key already exists, that item
// 若已存在同 key 的旧项，则使用新项替换它，并返回旧项。
// will be replaced with the new one, and return the previous item.
// （返回值是被替换掉的旧项。）
func (db *DB) insertIntoDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	// Generate a list of indexes that this item will be inserted in to.
	// 生成该数据项将要插入的索引列表。
	idxs := db.insIdxs
	for _, idx := range db.idxs {
		if idx.match(item.key) {
			idxs = append(idxs, idx)
		}
	}
	prev := db.keys.Set(item)
	if prev != nil {
		// A previous item was removed from the keys tree. Let's
		// 旧项已从 keys 树移除；需要把该项从所有索引中彻底删除。
		// fully delete this item from all indexes.
		pdbi = prev.(*dbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the expires tree.
			// 从 expires 过期树中移除。
			db.exps.Delete(pdbi)
		}
		for _, idx := range idxs {
			if idx.btr != nil {
				// Remove it from the btree index.
				// 从 btree 索引中移除。
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				// 从 rtree 索引中移除。
				idx.rtr.Remove(pdbi)
			}
		}
	}
	if item.opts != nil && item.opts.ex {
		// The new item has eviction options. Add it to the
		// 新项配置了驱逐/过期选项，则加入 expires 树。
		// expires tree
		db.exps.Set(item)
	}
	for i, idx := range idxs {
		if idx.btr != nil {
			// Add new item to btree index.
			// 将新项加入 btree 索引。
			idx.btr.Set(item)
		}
		if idx.rtr != nil {
			// Add new item to rtree index.
			// 将新项加入 rtree 索引。
			idx.rtr.Insert(item)
		}
		// clear the index
		// 清空该复用槽，避免重复使用错误。
		idxs[i] = nil
	}
	// reuse the index list slice
	// 复用索引列表的切片。
	db.insIdxs = idxs[:0]
	// we must return the previous item to the caller.
	// 返回旧项给调用方。
	return pdbi
}

// deleteFromDatabase removes and item from the database and indexes. The input
// deleteFromDatabase 会从数据库和索引中移除指定项。
// item must only have the key field specified thus "&dbItem{key: key}" is all
// 调用方只需要提供 key 字段，因此 `&dbItem{key: key}` 就足够了。
// that is needed to fully remove the item with the matching key. If an item
// 用于完整移除匹配 key 的那一项。
// with the matching key was found in the database, it will be removed and
// 若数据库中确实找到匹配项，它将被移除并返回给调用方。
// returned to the caller. A nil return value means that the item was not
// A nil 返回值表示没有找到该项。
// found in the database
// （因此也不会发生移除。）
func (db *DB) deleteFromDatabase(item *dbItem) *dbItem {
	var pdbi *dbItem
	prev := db.keys.Delete(item)
	if prev != nil {
		pdbi = prev.(*dbItem)
		if pdbi.opts != nil && pdbi.opts.ex {
			// Remove it from the exipres tree.
			// 从 expires（过期）树中移除。
			db.exps.Delete(pdbi)
		}
		for _, idx := range db.idxs {
			if !idx.match(pdbi.key) {
				continue
			}
			if idx.btr != nil {
				// Remove it from the btree index.
				// 从 btree 索引中移除。
				idx.btr.Delete(pdbi)
			}
			if idx.rtr != nil {
				// Remove it from the rtree index.
				// 从 rtree 索引中移除。
				idx.rtr.Remove(pdbi)
			}
		}
	}
	return pdbi
}

// backgroundManager runs continuously in the background and performs various
// backgroundManager 在后台持续运行，并执行各种操作。
// operations such as removing expired items and syncing to disk.
// 例如移除已过期的项，以及执行磁盘同步。
func (db *DB) backgroundManager() {
	flushes := 0
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		var shrink bool
		// Open a standard view. This will take a full lock of the
		// 打开一个“标准视图（view）”，会对数据库加上完整锁。
		// database thus allowing for access to anything we need.
		// 这样才能安全地访问我们需要的任意数据。
		var onExpired func([]string)
		var expired []*dbItem
		var onExpiredSync func(key, value string, tx *Tx) error
		err := db.Update(func(tx *Tx) error {
			onExpired = db.config.OnExpired
			if onExpired == nil {
				onExpiredSync = db.config.OnExpiredSync
			}
			if db.persist && !db.config.AutoShrinkDisabled {
				pos, err := db.file.Seek(0, 1)
				if err != nil {
					return err
				}
				aofsz := int(pos)
				if aofsz > db.config.AutoShrinkMinSize {
					prc := float64(db.config.AutoShrinkPercentage) / 100.0
					shrink = aofsz > db.lastaofsz+int(float64(db.lastaofsz)*prc)
				}
			}
			// produce a list of expired items that need removing
			// 生成一份需要移除的过期项列表。
			btreeAscendLessThan(db.exps, &dbItem{
				opts: &dbItemOpts{ex: true, exat: time.Now()},
			}, func(item interface{}) bool {
				expired = append(expired, item.(*dbItem))
				return true
			})
			if onExpired == nil && onExpiredSync == nil {
				for _, itm := range expired {
					if _, err := tx.Delete(itm.key); err != nil {
						// it's ok to get a "not found" because the
						// 之所以“未找到”也没问题，是因为：
						// 'Delete' method reports "not found" for
						// `Delete` 在处理过期项时会报告“not found”。
						// expired items.
						// （这些过期项本来可能已被移除。）
						if err != ErrNotFound {
							return err
						}
					}
				}
			} else if onExpiredSync != nil {
				for _, itm := range expired {
					if err := onExpiredSync(itm.key, itm.val, tx); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err == ErrDatabaseClosed {
			break
		}

		// send expired event, if needed
		// 若需要，则发送“过期事件”。
		if onExpired != nil && len(expired) > 0 {
			keys := make([]string, 0, 32)
			for _, itm := range expired {
				keys = append(keys, itm.key)
			}
			onExpired(keys)
		}

		// execute a disk sync, if needed
		// 需要时执行磁盘同步。
		func() {
			db.mu.Lock()
			defer db.mu.Unlock()
			if db.persist && db.config.SyncPolicy == EverySecond &&
				flushes != db.flushes {
				_ = db.file.Sync()
				flushes = db.flushes
			}
		}()
		if shrink {
			if err = db.Shrink(); err != nil {
				if err == ErrDatabaseClosed {
					break
				}
			}
		}
	}
}

// Shrink will make the database file smaller by removing redundant
// Shrink 通过删除冗余日志来缩小数据库文件。
// log entries. This operation does not block the database.
// 该操作不会阻塞数据库。
func (db *DB) Shrink() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return ErrDatabaseClosed
	}
	if !db.persist {
		// The database was opened with ":memory:" as the path.
		// 数据库以 `:memory:` 作为路径打开，因此没有持久化。
		// There is no persistence, and no need to do anything here.
		// 既然没有持久化，这里也不需要做任何处理。
		db.mu.Unlock()
		return nil
	}
	if db.shrinking {
		// The database is already in the process of shrinking.
		// 数据库正在进行缩容中。
		db.mu.Unlock()
		return ErrShrinkInProcess
	}
	db.shrinking = true
	defer func() {
		db.mu.Lock()
		db.shrinking = false
		db.mu.Unlock()
	}()
	fname := db.file.Name()
	tmpname := fname + ".tmp"
	// the endpos is used to return to the end of the file when we are
	// 当我们写完当前所有数据项后，会用它回到文件末尾。
	// finished writing all of the current items.
	// （用于后续拷贝新命令。）
	endpos, err := db.file.Seek(0, 2)
	if err != nil {
		return err
	}
	db.mu.Unlock()
	time.Sleep(time.Second / 4) // wait just a bit before starting
	// 在开始前稍等片刻。
	f, err := os.Create(tmpname)
	if err != nil {
		return err
	}
	defer func() {
		_ = f.Close()
		_ = os.RemoveAll(tmpname)
	}()

	// we are going to read items in as chunks as to not hold up the database
	// 这里会按块（chunks）读取数据，以避免长时间占用数据库。
	// for too long.
	// （确保数据库不会被阻塞太久。）
	var buf []byte
	pivot := ""
	done := false
	for !done {
		err := func() error {
			db.mu.RLock()
			defer db.mu.RUnlock()
			if db.closed {
				return ErrDatabaseClosed
			}
			done = true
			var n int
			now := time.Now()
			btreeAscendGreaterOrEqual(db.keys, &dbItem{key: pivot},
				func(item interface{}) bool {
					dbi := item.(*dbItem)
					// 1000 items or 64MB buffer
					// 每次批处理最多 1000 个 item，或缓冲区超过 64MB 就停止继续。
					if n > 1000 || len(buf) > 64*1024*1024 {
						pivot = dbi.key
						done = false
						return false
					}
					buf = dbi.writeSetTo(buf, now)
					n++
					return true
				},
			)
			if len(buf) > 0 {
				if _, err := f.Write(buf); err != nil {
					return err
				}
				buf = buf[:0]
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	// We reached this far so all of the items have been written to a new tmp
	// 到这里为止，所有项都已经写入了新的 tmp 文件。
	// There's some more work to do by appending the new line from the aof
	// 接下来还需要把 aof 里自缩容开始后的新增命令追加到 tmp 中，
	// to the tmp file and finally swap the files out.
	// 然后交换文件，完成缩容。
	return func() error {
		// We're wrapping this in a function to get the benefit of a defered
		// 用函数包裹是为了利用 defer 做锁/解锁。
		// lock/unlock.
		// （在退出时自动解锁。）
		db.mu.Lock()
		defer db.mu.Unlock()
		if db.closed {
			return ErrDatabaseClosed
		}
		// We are going to open a new version of the aof file so that we do
		// 重新打开一个新的 aof 版本文件，这样不会改变旧文件的 seek 位置。
		// not change the seek position of the previous. This may cause a
		// 如果未来使用 syscall 文件锁定，可能会引发问题。
		// problem in the future if we choose to use syscall file locking.
		// （因此这里先避免修改 seek。）
		aof, err := os.Open(fname)
		if err != nil {
			return err
		}
		defer func() { _ = aof.Close() }()
		if _, err := aof.Seek(endpos, 0); err != nil {
			return err
		}
		// Just copy all of the new commands that have occurred since we
		// 复制缩容开始之后产生的新命令。
		// started the shrink process.
		// （追加到 tmp 中。）
		if _, err := io.Copy(f, aof); err != nil {
			return err
		}
		// Close all files
		// 关闭所有文件。
		if err := aof.Close(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		if err := db.file.Close(); err != nil {
			return err
		}
		// Any failures below here are really bad. So just panic.
		// 下面的任何失败都非常糟糕，因此直接 panic。
		if err := renameFile(tmpname, fname); err != nil {
			panicErr(err)
		}
		db.file, err = os.OpenFile(fname, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			panicErr(err)
		}
		pos, err := db.file.Seek(0, 2)
		if err != nil {
			return err
		}
		db.lastaofsz = int(pos)
		return nil
	}()
}

func panicErr(err error) error {
	panic(fmt.Errorf("buntdb: %w", err))
}

func renameFile(src, dest string) error {
	var err error
	if err = os.Rename(src, dest); err != nil {
		if runtime.GOOS == "windows" {
			if err = os.Remove(dest); err == nil {
				err = os.Rename(src, dest)
			}
		}
	}
	return err
}

// readLoad reads from the reader and loads commands into the database.
// modTime is the modified time of the reader, should be no greater than
// the current time.Now().
// Returns the number of bytes of the last command read and the error if any.
// readLoad 从 `rd` 读取命令并加载到数据库。
// modTime 为 reader 的修改时间，应不晚于 `time.Now()`。
// 返回最后一条命令读取的字节数，以及发生的错误（若有）。
func (db *DB) readLoad(rd io.Reader, modTime time.Time) (n int64, err error) {
	defer func() {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
	}()
	totalSize := int64(0)
	data := make([]byte, 4096)
	parts := make([]string, 0, 8)
	r := bufio.NewReader(rd)
	for {
		// peek at the first byte. If it's a 'nul' control character then
		// ignore it and move to the next byte.
		// 先查看首字节：如果是 `nul` 控制字符，就忽略并继续读取下一字节。
		c, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return totalSize, err
		}
		if c == 0 {
			// ignore nul control characters
			// 忽略 `nul` 控制字符。
			n += 1
			continue
		}
		if err := r.UnreadByte(); err != nil {
			return totalSize, err
		}

		// read a single command.
		// first we should read the number of parts that the of the command
		// 读取一条完整命令：首先读取该命令一共有多少个部分（parts）。
		cmdByteSize := int64(0)
		line, err := r.ReadBytes('\n')
		if err != nil {
			return totalSize, err
		}
		if line[0] != '*' {
			return totalSize, ErrInvalid
		}
		cmdByteSize += int64(len(line))

		// convert the string number to and int
		// 把读到的字符串数字转换为整数。
		var n int
		if len(line) == 4 && line[len(line)-2] == '\r' {
			if line[1] < '0' || line[1] > '9' {
				return totalSize, ErrInvalid
			}
			n = int(line[1] - '0')
		} else {
			if len(line) < 5 || line[len(line)-2] != '\r' {
				return totalSize, ErrInvalid
			}
			for i := 1; i < len(line)-2; i++ {
				if line[i] < '0' || line[i] > '9' {
					return totalSize, ErrInvalid
				}
				n = n*10 + int(line[i]-'0')
			}
		}
		// read each part of the command.
		// 逐个读取命令的每个 part。
		parts = parts[:0]
		for i := 0; i < n; i++ {
			// read the number of bytes of the part.
			// 读取当前 part 的字节长度。
			line, err := r.ReadBytes('\n')
			if err != nil {
				return totalSize, err
			}
			if line[0] != '$' {
				return totalSize, ErrInvalid
			}
			cmdByteSize += int64(len(line))
			// convert the string number to and int
			// 再把当前 part 的字符串数字转换为整数长度。
			var n int
			if len(line) == 4 && line[len(line)-2] == '\r' {
				if line[1] < '0' || line[1] > '9' {
					return totalSize, ErrInvalid
				}
				n = int(line[1] - '0')
			} else {
				if len(line) < 5 || line[len(line)-2] != '\r' {
					return totalSize, ErrInvalid
				}
				for i := 1; i < len(line)-2; i++ {
					if line[i] < '0' || line[i] > '9' {
						return totalSize, ErrInvalid
					}
					n = n*10 + int(line[i]-'0')
				}
			}
			// resize the read buffer
			// 根据需要扩容读取缓冲区。
			if len(data) < n+2 {
				dataln := len(data)
				for dataln < n+2 {
					dataln *= 2
				}
				data = make([]byte, dataln)
			}
			if _, err = io.ReadFull(r, data[:n+2]); err != nil {
				return totalSize, err
			}
			if data[n] != '\r' || data[n+1] != '\n' {
				return totalSize, ErrInvalid
			}
			// copy string
			// 复制 part 内容到字符串数组中。
			parts = append(parts, string(data[:n]))
			cmdByteSize += int64(n + 2)
		}
		// finished reading the command
		// 命令读取完成。

		if len(parts) == 0 {
			continue
		}
		if (parts[0][0] == 's' || parts[0][0] == 'S') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 't' || parts[0][2] == 'T') {
			// SET
			// 处理 SET 命令。
			if len(parts) < 3 || len(parts) == 4 || len(parts) > 5 {
				return totalSize, ErrInvalid
			}
			if len(parts) == 5 {
				arg := strings.ToLower(parts[3])
				if arg != "ex" && arg != "ae" {
					return totalSize, ErrInvalid
				}
				ex, err := strconv.ParseInt(parts[4], 10, 64)
				if err != nil {
					return totalSize, err
				}
				var exat time.Time
				now := time.Now()
				if arg == "ex" {
					dur := (time.Duration(ex) * time.Second) - now.Sub(modTime)
					exat = now.Add(dur)
				} else {
					exat = time.Unix(ex, 0)
				}
				if exat.After(now) {
					db.insertIntoDatabase(&dbItem{
						key: parts[1],
						val: parts[2],
						opts: &dbItemOpts{
							ex:   true,
							exat: exat,
						},
					})
				} else {
					db.deleteFromDatabase(&dbItem{
						key: parts[1],
					})
				}
			} else {
				db.insertIntoDatabase(&dbItem{key: parts[1], val: parts[2]})
			}
		} else if (parts[0][0] == 'd' || parts[0][0] == 'D') &&
			(parts[0][1] == 'e' || parts[0][1] == 'E') &&
			(parts[0][2] == 'l' || parts[0][2] == 'L') {
			// DEL
			// 处理 DEL 命令。
			if len(parts) != 2 {
				return totalSize, ErrInvalid
			}
			db.deleteFromDatabase(&dbItem{key: parts[1]})
		} else if (parts[0][0] == 'f' || parts[0][0] == 'F') &&
			strings.ToLower(parts[0]) == "flushdb" {
			db.keys = btreeNew(lessCtx(nil))
			db.exps = btreeNew(lessCtx(&exctx{db}))
			db.idxs = make(map[string]*index)
		} else {
			return totalSize, ErrInvalid
		}
		totalSize += cmdByteSize
	}
}

// load reads entries from the append only database file and fills the database.
// The file format uses the Redis append only file format, which is and a series
// of RESP commands. For more information on RESP please read
// http://redis.io/topics/protocol. The only supported RESP commands are DEL and
// SET.
// load 从仅追加的数据库文件中读取记录并填充到内存数据库。
// 该文件格式使用 Redis 的 AOF/RDB 类似结构：由一系列 RESP 命令组成。
// RESP 详细协议请参考 `http://redis.io/topics/protocol`。
// 目前仅支持 `DEL` 和 `SET` 两类 RESP 命令。
func (db *DB) load() error {
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	n, err := db.readLoad(db.file, fi.ModTime())
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			// The db file has ended mid-command, which is allowed but the
			// data file should be truncated to the end of the last valid
			// command
			// 数据文件在命令中途结束（但这是允许的）；需要把文件截断到最后一条有效命令的末尾。
			if err := db.file.Truncate(n); err != nil {
				return err
			}
		} else {
			return err
		}
	}
	if _, err := db.file.Seek(n, 0); err != nil {
		return err
	}
	var estaofsz int
	db.keys.Walk(func(items []interface{}) {
		for _, v := range items {
			estaofsz += v.(*dbItem).estAOFSetSize()
		}
	})
	db.lastaofsz += estaofsz
	return nil
}

// managed calls a block of code that is fully contained in a transaction.
// managed 以事务为边界执行一段完整代码。
// This method is intended to be wrapped by Update and View
// 该方法由 `Update` 和 `View` 进行封装调用。
func (db *DB) managed(writable bool, fn func(tx *Tx) error) (err error) {
	var tx *Tx
	tx, err = db.Begin(writable)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			// The caller returned an error. We must rollback.
			// 调用方返回了错误，需要回滚事务。
			_ = tx.Rollback()
			return
		}
		if writable {
			// Everything went well. Lets Commit()
			// 一切正常，提交事务（Commit）。
			err = tx.Commit()
		} else {
			// read-only transaction can only roll back.
			// 只读事务只能回滚（Rollback）。
			err = tx.Rollback()
		}
	}()
	tx.funcd = true
	defer func() {
		tx.funcd = false
	}()
	err = fn(tx)
	return
}

// View executes a function within a managed read-only transaction.
// View 在“托管的只读事务”中执行一个函数。
// When a non-nil error is returned from the function that error will be return
// 如果函数返回非 nil 错误，将返回给 `View()` 的调用方。
// to the caller of View().
//
// Executing a manual commit or rollback from inside the function will result
// 在函数内部手动调用 Commit/Rollback 会导致 panic。
// in a panic.
func (db *DB) View(fn func(tx *Tx) error) error {
	return db.managed(false, fn)
}

// Update executes a function within a managed read/write transaction.
// Update 在“托管的读写事务”中执行一个函数。
// The transaction has been committed when no error is returned.
// 如果没有错误返回，则提交事务（Commit）。
// In the event that an error is returned, the transaction will be rolled back.
// 如果函数返回错误，则事务会回滚（Rollback）。
// When a non-nil error is returned from the function, the transaction will be
// 当函数返回非 nil 错误时，事务会回滚，并把该错误返回给调用方。
// rolled back and the that error will be return to the caller of Update().
//
// Executing a manual commit or rollback from inside the function will result
// 在函数内部手动调用 Commit/Rollback 会导致 panic。
// in a panic.
func (db *DB) Update(fn func(tx *Tx) error) error {
	return db.managed(true, fn)
}

// get return an item or nil if not found.
// get 返回匹配到的 item，未找到则返回 nil。
func (db *DB) get(key string) *dbItem {
	item := db.keys.Get(&dbItem{key: key})
	if item != nil {
		return item.(*dbItem)
	}
	return nil
}

// Tx represents a transaction on the database. This transaction can either be
// Tx 表示数据库上的一个事务，可以是只读或读写。
// read-only or read/write. Read-only transactions can be used for retrieving
// 只读事务可用于读取 key 的值并遍历 key/value。
// values for keys and iterating through keys and values. Read/write
// 读写事务可用于设置与删除 key。
// transactions can set and delete keys.
//
// All transactions must be committed or rolled-back when done.
// 事务结束后必须 Commit 或 Rollback。
type Tx struct {
	db       *DB             // the underlying database.
	// 底层数据库引用。
	writable bool            // when false mutable operations fail.
	// writable=false 时，可变操作会失败。
	funcd    bool            // when true Commit and Rollback panic.
	// funcd=true 时，Commit/Rollback 会 panic。
	wc       *txWriteContext // context for writable transactions.
	// 可写事务的写入上下文。
}

type txWriteContext struct {
	// rollback when deleteAll is called
	// 当调用 DeleteAll 时，需要在回滚过程中恢复状态。
	rbkeys *btree.BTree      // a tree of all item ordered by key
	// 按 key 排序的备份 keys 树。
	rbexps *btree.BTree      // a tree of items ordered by expiration
	// 按过期时间排序的备份 exps 树。
	rbidxs map[string]*index // the index trees.
	// 索引备份集合。

	rollbackItems   map[string]*dbItem // details for rolling back tx.
	// 用于回滚事务的逐项变更记录。
	commitItems     map[string]*dbItem // details for committing tx.
	// 用于事务提交写入磁盘的逐项记录。
	itercount       int                // stack of iterators
	// 迭代器层级计数（避免迭代期间修改）。
	rollbackIndexes map[string]*index  // details for dropped indexes.
	// 被删除索引的回滚/重建信息。
}

// DeleteAll deletes all items from the database.
// DeleteAll 会删除数据库中的所有数据项。
func (tx *Tx) DeleteAll() error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}

	// check to see if we've already deleted everything
	// 先判断是否已经删除过全部数据。
	if tx.wc.rbkeys == nil {
		// we need to backup the live data in case of a rollback.
		// 为了在回滚时恢复，需要备份当前“在线”数据结构。
		tx.wc.rbkeys = tx.db.keys
		tx.wc.rbexps = tx.db.exps
		tx.wc.rbidxs = tx.db.idxs
	}

	// now reset the live database trees
	// 现在重置“在线”数据结构（keys/exps/idxs）。
	tx.db.keys = btreeNew(lessCtx(nil))
	tx.db.exps = btreeNew(lessCtx(&exctx{tx.db}))
	tx.db.idxs = make(map[string]*index)

	// finally re-create the indexes
	// 最后重新创建索引（从备份中拷贝索引元信息）。
	for name, idx := range tx.wc.rbidxs {
		tx.db.idxs[name] = idx.clearCopy()
	}

	// always clear out the commits
	// 清空当前事务的提交记录（commitItems）。
	tx.wc.commitItems = make(map[string]*dbItem)

	return nil
}

// Begin opens a new transaction.
// Begin 打开一个新的事务。
// Multiple read-only transactions can be opened at the same time but there can
// 可以同时打开多个只读事务，但读写事务一次只能有一个。
// only be one read/write transaction at a time. Attempting to open a read/write
// 如果在另一个读写事务进行中尝试打开新的读写事务，会阻塞直到当前读写事务完成。
// transactions while another one is in progress will result in blocking until
// the current read/write transaction is completed.
// （见上面的说明：等待直到当前读写事务结束。）
//
// All transactions must be closed by calling Commit() or Rollback() when done.
// 事务结束后必须通过 Commit 或 Rollback 关闭。
func (db *DB) Begin(writable bool) (*Tx, error) {
	tx := &Tx{
		db:       db,
		writable: writable,
	}
	tx.lock()
	if db.closed {
		tx.unlock()
		return nil, ErrDatabaseClosed
	}
	if writable {
		// writable transactions have a writeContext object that
		// contains information about changes to the database.
		// 读写事务会有一个 writeContext，用于记录对数据库的修改。
		tx.wc = &txWriteContext{}
		tx.wc.rollbackItems = make(map[string]*dbItem)
		tx.wc.rollbackIndexes = make(map[string]*index)
		if db.persist {
			tx.wc.commitItems = make(map[string]*dbItem)
		}
	}
	return tx, nil
}

// lock locks the database based on the transaction type.
// lock 根据事务类型对数据库加锁：只读用读锁，读写用写锁。
func (tx *Tx) lock() {
	if tx.writable {
		tx.db.mu.Lock()
	} else {
		tx.db.mu.RLock()
	}
}

// unlock unlocks the database based on the transaction type.
// unlock 根据事务类型解锁。
func (tx *Tx) unlock() {
	if tx.writable {
		tx.db.mu.Unlock()
	} else {
		tx.db.mu.RUnlock()
	}
}

// rollbackInner handles the underlying rollback logic.
// rollbackInner 执行底层回滚逻辑。
// Intended to be called from Commit() and Rollback().
// Intended to be called from Commit() and Rollback()（由 Commit/Rollback 调用）。
func (tx *Tx) rollbackInner() {
	// rollback the deleteAll if needed
	// 如果需要，回滚 DeleteAll 对在线数据结构造成的影响。
	if tx.wc.rbkeys != nil {
		tx.db.keys = tx.wc.rbkeys
		tx.db.idxs = tx.wc.rbidxs
		tx.db.exps = tx.wc.rbexps
	}
	for key, item := range tx.wc.rollbackItems {
		tx.db.deleteFromDatabase(&dbItem{key: key})
		if item != nil {
			// When an item is not nil, we will need to reinsert that item
			// into the database overwriting the current one.
			// 当 item 不为 nil 时，需要把该 item 重新插回数据库，用于覆盖当前值。
			tx.db.insertIntoDatabase(item)
		}
	}
	for name, idx := range tx.wc.rollbackIndexes {
		delete(tx.db.idxs, name)
		if idx != nil {
			// When an index is not nil, we will need to rebuilt that index
			// this could be an expensive process if the database has many
			// items or the index is complex.
			// 当 idx 不为 nil 时，需要重建该索引；若数据项多或索引复杂，代价会较高。
			tx.db.idxs[name] = idx
			idx.rebuild()
		}
	}
}

// Commit writes all changes to disk.
// Commit 会把事务中的所有更改写入磁盘。
// An error is returned when a write error occurs, or when a Commit() is called
// 如果发生写入错误，或在只读事务中调用 Commit()，则返回错误。
// from a read-only transaction.
func (tx *Tx) Commit() error {
	if tx.funcd {
		panic("managed tx commit not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}
	var err error
	if tx.db.persist && (len(tx.wc.commitItems) > 0 || tx.wc.rbkeys != nil) {
		tx.db.buf = tx.db.buf[:0]
		// write a flushdb if a deleteAll was called.
		// 如果调用过 DeleteAll，则先写入 flushdb 以清空持久化日志。
		if tx.wc.rbkeys != nil {
			tx.db.buf = append(tx.db.buf, "*1\r\n$7\r\nflushdb\r\n"...)
		}
		now := time.Now()
		// Each committed record is written to disk
		// 每一条提交记录都会写入磁盘。
		for key, item := range tx.wc.commitItems {
			if item == nil {
				tx.db.buf = (&dbItem{key: key}).writeDeleteTo(tx.db.buf)
			} else {
				tx.db.buf = item.writeSetTo(tx.db.buf, now)
			}
		}
		// Flushing the buffer only once per transaction.
		// 缓冲区仅在每个事务中刷新一次。
		// If this operation fails then the write did failed and we must
		// rollback.
		// 如果这一步失败，写入就不可靠，需要回滚事务。
		var n int
		n, err = tx.db.file.Write(tx.db.buf)
		if err != nil {
			if n > 0 {
				// There was a partial write to disk.
				// 发生了磁盘的“部分写入”。
				// We are possibly out of disk space.
				// Delete the partially written bytes from the data file by
				// seeking to the previously known position and performing
				// a truncate operation.
				// At this point a syscall failure is fatal and the process
				// should be killed to avoid corrupting the file.
				// 此时 syscall 失败属于致命错误，应终止进程以避免文件损坏。
				pos, err := tx.db.file.Seek(-int64(n), 1)
				if err != nil {
					panicErr(err)
				}
				if err := tx.db.file.Truncate(pos); err != nil {
					panicErr(err)
				}
			}
			tx.rollbackInner()
		}
		if tx.db.config.SyncPolicy == Always {
			_ = tx.db.file.Sync()
		}
		// Increment the number of flushes. The background syncing uses this.
	// 增加 flush 次数；后台同步会使用该计数。
		tx.db.flushes++
	}
	// Unlock the database and allow for another writable transaction.
// 解锁数据库，允许新的可写事务开始。
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
// 清空 tx.db，避免该事务在后续被再次使用。
	tx.db = nil
	return err
}

// Rollback closes the transaction and reverts all mutable operations that
// Rollback 会撤销事务中执行的所有可变操作（例如 Set/Delete）。
// were performed on the transaction such as Set() and Delete().
//
// Read-only transactions can only be rolled back, not committed.
// 只读事务只能回滚，不能提交（Commit）。
func (tx *Tx) Rollback() error {
	if tx.funcd {
		panic("managed tx rollback not allowed")
	}
	if tx.db == nil {
		return ErrTxClosed
	}
	// The rollback func does the heavy lifting.
	// 回滚函数承担主要的恢复工作。
	if tx.writable {
		tx.rollbackInner()
	}
	// unlock the database for more transactions.
	// 解锁数据库，允许更多事务继续执行。
	tx.unlock()
	// Clear the db field to disable this transaction from future use.
	// 清空 tx.db，避免该事务在未来再次被使用。
	tx.db = nil
	return nil
}

// dbItemOpts holds various meta information about an item.
// dbItemOpts 存放与数据项相关的各种元信息（例如是否过期及过期时间）。
type dbItemOpts struct {
	ex   bool      // does this item expire?
	// 是否需要过期。
	exat time.Time // when does this item expire?
	// 过期的绝对时间点。
}
type dbItem struct {
	key, val string      // the binary key and value
	// 二进制形式的 key 与 value。
	opts     *dbItemOpts // optional meta information
	// 可选的元信息（例如过期策略）。
	keyless  bool        // keyless item for scanning
	// 用于扫描边界的“无 key”标记项。
}

// estIntSize returns the string representions size.
// estIntSize 返回整数转换成字符串后所需的长度。
// Has the same result as len(strconv.Itoa(x)).
// 与 `len(strconv.Itoa(x))` 的结果一致。
func estIntSize(x int) int {
	n := 1
	if x < 0 {
		n++
		x *= -1
	}
	for x >= 10 {
		n++
		x /= 10
	}
	return n
}

func estArraySize(count int) int {
	return 1 + estIntSize(count) + 2
}

func estBulkStringSize(s string) int {
	return 1 + estIntSize(len(s)) + 2 + len(s) + 2
}

// estAOFSetSize returns an estimated number of bytes that this item will use
// estAOFSetSize 估算该 item 在 aof 文件中将占用的字节数。
// when stored in the aof file.
func (dbi *dbItem) estAOFSetSize() int {
	var n int
	if dbi.opts != nil && dbi.opts.ex {
		n += estArraySize(5)
		n += estBulkStringSize("set")
		n += estBulkStringSize(dbi.key)
		n += estBulkStringSize(dbi.val)
		n += estBulkStringSize("ex")
		n += estBulkStringSize("99") // estimate two byte bulk string
		// 估算两个字节长度的 bulk string。
	} else {
		n += estArraySize(3)
		n += estBulkStringSize("set")
		n += estBulkStringSize(dbi.key)
		n += estBulkStringSize(dbi.val)
	}
	return n
}

func appendArray(buf []byte, count int) []byte {
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(count), 10)
	buf = append(buf, '\r', '\n')
	return buf
}

func appendBulkString(buf []byte, s string) []byte {
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(s)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, s...)
	buf = append(buf, '\r', '\n')
	return buf
}

// writeSetTo writes an item as a single SET record to the a bufio Writer.
// writeSetTo 将一个 item 以单条 `SET` 记录写入到 bufio Writer。
func (dbi *dbItem) writeSetTo(buf []byte, now time.Time) []byte {
	if dbi.opts != nil && dbi.opts.ex {
		buf = appendArray(buf, 5)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkString(buf, dbi.val)
		if useAbsEx {
			ex := dbi.opts.exat.Unix()
			buf = appendBulkString(buf, "ae")
			buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
		} else {
			ex := dbi.opts.exat.Sub(now) / time.Second
			buf = appendBulkString(buf, "ex")
			buf = appendBulkString(buf, strconv.FormatUint(uint64(ex), 10))
		}
	} else {
		buf = appendArray(buf, 3)
		buf = appendBulkString(buf, "set")
		buf = appendBulkString(buf, dbi.key)
		buf = appendBulkString(buf, dbi.val)
	}
	return buf
}

// writeSetTo writes an item as a single DEL record to the a bufio Writer.
// writeDeleteTo（该注释原文为 writeSetTo）将一个 item 以单条 `DEL` 记录写入 bufio Writer。
func (dbi *dbItem) writeDeleteTo(buf []byte) []byte {
	buf = appendArray(buf, 2)
	buf = appendBulkString(buf, "del")
	buf = appendBulkString(buf, dbi.key)
	return buf
}

// expired evaluates id the item has expired. This will always return false when
// expired 用于判断 item 是否已过期；当未设置 `opts.ex` 为 true 时一定返回 false。
// the item does not have `opts.ex` set to true.
func (dbi *dbItem) expired() bool {
	return dbi.opts != nil && dbi.opts.ex && time.Now().After(dbi.opts.exat)
}

// MaxTime from http://stackoverflow.com/questions/25065055#32620397
// MaxTime 取自该链接：一个远未来时间，用于 b-tree 排序。
// This is a long time in the future. It's an imaginary number that is
// used for b-tree ordering.
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// expiresAt will return the time when the item will expire. When an item does
// expiresAt 返回 item 将过期的时间；如果不会过期，则返回 `maxTime`。
// not expire `maxTime` is used.
func (dbi *dbItem) expiresAt() time.Time {
	if dbi.opts == nil || !dbi.opts.ex {
		return maxTime
	}
	return dbi.opts.exat
}

// Less determines if a b-tree item is less than another. This is required
// Less 用于判断一个 b-tree item 是否“小于”另一个，用于排序、插入与删除。
// for ordering, inserting, and deleting items from a b-tree. It's important
// 其中 ctx 参数用于决定采用哪一种比较公式；即使共享同一 item，不同 b-tree 也应使用不同 ctx。
// to note that the ctx parameter is used to help with determine which
// formula to use on an item. Each b-tree should use a different ctx when
// sharing the same item.
func (dbi *dbItem) Less(dbi2 *dbItem, ctx interface{}) bool {
	switch ctx := ctx.(type) {
	case *exctx:
		// The expires b-tree formula
		// 这里使用“按过期时间（expires）的 b-tree 比较公式”。
		if dbi2.expiresAt().After(dbi.expiresAt()) {
			return true
		}
		if dbi.expiresAt().After(dbi2.expiresAt()) {
			return false
		}
	case *index:
		if ctx.less != nil {
			// Using an index
			// 使用自定义索引的 less 比较函数。
			if ctx.less(dbi.val, dbi2.val) {
				return true
			}
			if ctx.less(dbi2.val, dbi.val) {
				return false
			}
		}
	}
	// Always fall back to the key comparison. This creates absolute uniqueness.
	// 如果以上都不适用，则回退到 key 的比较；这样可以保证绝对唯一性。
	if dbi.keyless {
		return false
	} else if dbi2.keyless {
		return true
	}
	return dbi.key < dbi2.key
}

func lessCtx(ctx interface{}) func(a, b interface{}) bool {
	return func(a, b interface{}) bool {
		return a.(*dbItem).Less(b.(*dbItem), ctx)
	}
}

// Rect converts a string to a rectangle.
// Rect 将一个字符串转换为一个矩形（rectangle）。
// An invalid rectangle will cause a panic.
func (dbi *dbItem) Rect(ctx interface{}) (min, max []float64) {
	switch ctx := ctx.(type) {
	case *index:
		return ctx.rect(dbi.val)
	}
	return nil, nil
}

// SetOptions represents options that may be included with the Set() command.
// SetOptions 表示 Set 命令可选的参数。
type SetOptions struct {
	// Expires indicates that the Set() key-value will expire
	// Expires 指示该 Set() 的 key-value 将过期。
	Expires bool
	// TTL is how much time the key-value will exist in the database
	// TTL 指定该 key-value 在数据库中存在的时长。
	// before being evicted. The Expires field must also be set to true.
	// TTL stands for Time-To-Live.
	TTL time.Duration
}

// GetLess returns the less function for an index. This is handy for
// GetLess 返回某个索引的 less 比较函数，便于在事务内进行临时比较（ad-hoc compares）。
// doing ad-hoc compares inside a transaction.
// Returns ErrNotFound if the index is not found or there is no less
// 当索引不存在，或该索引没有绑定 less 函数时返回 ErrNotFound。
// function bound to the index
func (tx *Tx) GetLess(index string) (func(a, b string) bool, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.idxs[index]
	if !ok || idx.less == nil {
		return nil, ErrNotFound
	}
	return idx.less, nil
}

// GetRect returns the rect function for an index. This is handy for
// GetRect 返回某个索引的 rect 函换函数，便于在事务内进行临时空间搜索。
// doing ad-hoc searches inside a transaction.
// Returns ErrNotFound if the index is not found or there is no rect
// 当索引不存在，或没有绑定 rect 函数时返回 ErrNotFound。
// function bound to the index
func (tx *Tx) GetRect(index string) (func(s string) (min, max []float64),
	error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	idx, ok := tx.db.idxs[index]
	if !ok || idx.rect == nil {
		return nil, ErrNotFound
	}
	return idx.rect, nil
}

// Set inserts or replaces an item in the database based on the key.
// Set 根据 key 插入或替换数据库中的一条记录。
// The opt params may be used for additional functionality such as forcing
// opt 参数可提供额外能力，例如在指定时间强制过期/驱逐该项。
// the item to be evicted at a specified time. When the return value
// 当 err 为 nil 时操作成功；当 replaced 为 true 时说明替换了已有项，并通过 previousValue 返回旧值。
// for err is nil the operation succeeded. When the return value of
// replaced is true, then the operaton replaced an existing item whose
// value will be returned through the previousValue variable.
// The results of this operation will not be available to other
// 该操作的结果在当前事务成功提交之前，对其他事务不可见。
// transactions until the current transaction has successfully committed.
//
// Only a writable transaction can be used with this operation.
// 该操作仅允许在可写事务中使用。
// This operation is not allowed during iterations such as Ascend* & Descend*.
// 在 Ascend*/Descend* 迭代过程中不允许调用该操作。
func (tx *Tx) Set(key, value string, opts *SetOptions) (previousValue string,
	replaced bool, err error) {
	if tx.db == nil {
		return "", false, ErrTxClosed
	} else if !tx.writable {
		return "", false, ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return "", false, ErrTxIterating
	}
	item := &dbItem{key: key, val: value}
	if opts != nil {
		if opts.Expires {
			// The caller is requesting that this item expires. Convert the
			// TTL to an absolute time and bind it to the item.
			// 调用方要求该 item 过期：把 TTL 转成绝对过期时间并绑定到该 item。
			item.opts = &dbItemOpts{ex: true, exat: time.Now().Add(opts.TTL)}
		}
	}
	// Insert the item into the keys tree.
	// 将 item 插入到 keys B-tree 中。
	prev := tx.db.insertIntoDatabase(item)

	// insert into the rollback map if there has not been a deleteAll.
	// 如果还没有调用过 DeleteAll，则在回滚映射中记录该变更，以便出现错误时恢复。
	if tx.wc.rbkeys == nil {
		if prev == nil {
			// An item with the same key did not previously exist. Let's
			// create a rollback entry with a nil value. A nil value indicates
			// that the entry should be deleted on rollback. When the value is
			// *not* nil, that means the entry should be reverted.
			// 之前不存在同 key 的 item：回滚时用 nil 作为标记，表示该条记录应当被删除。
			if _, ok := tx.wc.rollbackItems[key]; !ok {
				tx.wc.rollbackItems[key] = nil
			}
		} else {
			// A previous item already exists in the database. Let's create a
			// rollback entry with the item as the value. We need to check the
			// map to see if there isn't already an item that matches the
			// same key.
			// 数据库中已有同 key 的旧 item：把旧 item 作为回滚值记录下来，以便回滚恢复。
			if _, ok := tx.wc.rollbackItems[key]; !ok {
				tx.wc.rollbackItems[key] = prev
			}
			if !prev.expired() {
				previousValue, replaced = prev.val, true
			}
		}
	}
	// For commits we simply assign the item to the map. We use this map to
	// write the entry to disk.
	// commit 时把 item 记录到 commitItems 映射，事务提交后会写入磁盘。
	if tx.db.persist {
		tx.wc.commitItems[key] = item
	}
	return previousValue, replaced, nil
}

// Get returns a value for a key. If the item does not exist or if the item
// Get 按 key 返回 value；如果 item 不存在或已过期则返回 ErrNotFound。
// has expired then ErrNotFound is returned. If ignoreExpired is true, then
// 如果 ignoreExpired 为 true，即使已过期也会返回找到的值。
// the found value will be returned even if it is expired.
func (tx *Tx) Get(key string, ignoreExpired ...bool) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	}
	var ignore bool
	if len(ignoreExpired) != 0 {
		ignore = ignoreExpired[0]
	}
	item := tx.db.get(key)
	if item == nil || (item.expired() && !ignore) {
		// The item does not exists or has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		// item 不存在或已过期：假设调用方只关心未过期的项。
		return "", ErrNotFound
	}
	return item.val, nil
}

// Delete removes an item from the database based on the item's key. If the item
// Delete 按 item 的 key 从数据库移除该项；如果 item 不存在或已过期则返回 ErrNotFound。
// does not exist or if the item has expired then ErrNotFound is returned.
//
// Only a writable transaction can be used for this operation.
// 仅可在可写事务中使用该操作。
// This operation is not allowed during iterations such as Ascend* & Descend*.
// 在 Ascend*/Descend* 迭代过程中不允许调用该操作。
func (tx *Tx) Delete(key string) (val string, err error) {
	if tx.db == nil {
		return "", ErrTxClosed
	} else if !tx.writable {
		return "", ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return "", ErrTxIterating
	}
	item := tx.db.deleteFromDatabase(&dbItem{key: key})
	if item == nil {
		return "", ErrNotFound
	}
	// create a rollback entry if there has not been a deleteAll call.
	// 如果还没有调用过 DeleteAll，则为本次删除创建回滚记录。
	if tx.wc.rbkeys == nil {
		if _, ok := tx.wc.rollbackItems[key]; !ok {
			tx.wc.rollbackItems[key] = item
		}
	}
	if tx.db.persist {
		tx.wc.commitItems[key] = nil
	}
	// Even though the item has been deleted, we still want to check
	// if it has expired. An expired item should not be returned.
	// 虽然已删除该 item，但仍需要检查它是否过期：过期项不应被返回。
	if item.expired() {
		// The item exists in the tree, but has expired. Let's assume that
		// the caller is only interested in items that have not expired.
		// item 在树中但已经过期：假设调用方只关心未过期项。
		return "", ErrNotFound
	}
	return item.val, nil
}

// TTL returns the remaining time-to-live for an item.
// TTL 返回某个 item 的剩余 TTL。
// A negative duration will be returned for items that do not have an
// 对没有设置过期时间的项，会返回负值。
// expiration.
func (tx *Tx) TTL(key string) (time.Duration, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	item := tx.db.get(key)
	if item == nil {
		return 0, ErrNotFound
	} else if item.opts == nil || !item.opts.ex {
		return -1, nil
	}
	dur := time.Until(item.opts.exat)
	if dur < 0 {
		return 0, ErrNotFound
	}
	return dur, nil
}

// scan iterates through a specified index and calls user-defined iterator
// scan 会遍历指定索引，并对遇到的每个 item 调用用户提供的 iterator 函数。
// function for each item encountered.
// The desc param indicates that the iterator should descend.
// desc 表示是否按降序遍历。
// The gt param indicates that there is a greaterThan limit.
// gt 表示存在 greaterThan 边界。
// The lt param indicates that there is a lessThan limit.
// lt 表示存在 lessThan 边界。
// The index param tells the scanner to use the specified index tree. An
// index 指定使用哪个索引树；index 为空字符串表示遍历 keys 而不是 values。
// empty string for the index means to scan the keys, not the values.
// The start and stop params are the greaterThan, lessThan limits. For
// start/stop 分别对应 greaterThan/lessThan；降序遍历时会互换为 lessThan/greaterThan。
// descending order, these will be lessThan, greaterThan.
// An error will be returned if the tx is closed or the index is not found.
func (tx *Tx) scan(desc, gt, lt bool, index, start, stop string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	// wrap a btree specific iterator around the user-defined iterator.
	// 把底层 btree 的迭代器包装成用户提供 iterator 的形式。
	iter := func(item interface{}) bool {
		dbi := item.(*dbItem)
		if dbi.expired() {
			return true
		}
		return iterator(dbi.key, dbi.val)
	}
	var tr *btree.BTree
	if index == "" {
		// empty index means we will use the keys tree.
		// index 为空意味着使用 keys 树（而非某个索引树）。
		tr = tx.db.keys
	} else {
		idx := tx.db.idxs[index]
		if idx == nil {
			// index was not found. return error
			// 索引不存在，返回 ErrNotFound。
			return ErrNotFound
		}
		tr = idx.btr
		if tr == nil {
			return nil
		}
	}
	// create some limit items
	// 创建边界用的 limit items（用于 range 查询）。
	var itemA, itemB *dbItem
	if gt || lt {
		if index == "" {
			itemA = &dbItem{key: start}
			itemB = &dbItem{key: stop}
		} else {
			itemA = &dbItem{val: start}
			itemB = &dbItem{val: stop}
			if desc {
				itemA.keyless = true
				itemB.keyless = true
			}
		}
	}
	// execute the scan on the underlying tree.
	// 在底层树上执行实际的 scan。
	if tx.wc != nil {
		tx.wc.itercount++
		defer func() {
			tx.wc.itercount--
		}()
	}
	if desc {
		if gt {
			if lt {
				btreeDescendRange(tr, itemA, itemB, iter)
			} else {
				btreeDescendGreaterThan(tr, itemA, iter)
			}
		} else if lt {
			btreeDescendLessOrEqual(tr, itemA, iter)
		} else {
			btreeDescend(tr, iter)
		}
	} else {
		if gt {
			if lt {
				btreeAscendRange(tr, itemA, itemB, iter)
			} else {
				btreeAscendGreaterOrEqual(tr, itemA, iter)
			}
		} else if lt {
			btreeAscendLessThan(tr, itemA, iter)
		} else {
			btreeAscend(tr, iter)
		}
	}
	return nil
}

// Match returns true if the specified key matches the pattern. This is a very
// Match 判断 key 是否匹配 pattern。它是一个简单模式匹配器：`*` 匹配任意数量字符，`?` 匹配任意单个字符。
// simple pattern matcher where '*' matches on any number characters and '?'
// matches on any one character.
func Match(key, pattern string) bool {
	return match.Match(key, pattern)
}

// AscendKeys allows for iterating through keys based on the specified pattern.
// AscendKeys 支持基于指定 pattern 遍历 keys。
func (tx *Tx) AscendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Ascend("", iterator)
		}
		return tx.Ascend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.AscendGreaterOrEqual("", min, func(key, value string) bool {
		if key > max {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// DescendKeys allows for iterating through keys based on the specified pattern.
// DescendKeys 支持基于指定 pattern 以降序遍历 keys。
func (tx *Tx) DescendKeys(pattern string,
	iterator func(key, value string) bool) error {
	if pattern == "" {
		return nil
	}
	if pattern[0] == '*' {
		if pattern == "*" {
			return tx.Descend("", iterator)
		}
		return tx.Descend("", func(key, value string) bool {
			if match.Match(key, pattern) {
				if !iterator(key, value) {
					return false
				}
			}
			return true
		})
	}
	min, max := match.Allowable(pattern)
	return tx.DescendLessOrEqual("", max, func(key, value string) bool {
		if key < min {
			return false
		}
		if match.Match(key, pattern) {
			if !iterator(key, value) {
				return false
			}
		}
		return true
	})
}

// Ascend calls the iterator for every item in the database within the range
// [first, last], until iterator returns false.
// Ascend 在范围 [first, last] 内依次调用 iterator；当 iterator 返回 false 时停止。
// When an index is provided, the results will be ordered by the item values
// 如果提供了 index，则结果按该 index 定义的 less() 函数对 value 排序。
// as specified by the less() function of the defined index.
// 当未提供 index 时，结果按 item 的 key 排序。
// When an index is not provided, the results will be ordered by the item key.
// 如果 index 无效会返回错误。
// An invalid index will return an error.
func (tx *Tx) Ascend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, false, index, "", "", iterator)
}

// AscendGreaterOrEqual calls the iterator for every item in the database within
// the range [pivot, last], until iterator returns false.
// AscendGreaterOrEqual 遍历范围 [pivot, last]（包含 pivot），直到 iterator 返回 false。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendGreaterOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, true, false, index, pivot, "", iterator)
}

// AscendLessThan calls the iterator for every item in the database within the
// range [first, pivot), until iterator returns false.
// AscendLessThan 遍历范围 [first, pivot)（不包含 pivot），直到 iterator 返回 false。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendLessThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(false, false, true, index, pivot, "", iterator)
}

// AscendRange calls the iterator for every item in the database within
// the range [greaterOrEqual, lessThan), until iterator returns false.
// AscendRange 遍历范围 [greaterOrEqual, lessThan)（包含 left， 不包含 right），直到 iterator 返回 false。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendRange(index, greaterOrEqual, lessThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		false, true, true, index, greaterOrEqual, lessThan, iterator,
	)
}

// Descend calls the iterator for every item in the database within the range
// [last, first], until iterator returns false.
// Descend 在范围 [last, first] 内按降序调用 iterator，直到 iterator 返回 false。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) Descend(index string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, false, index, "", "", iterator)
}

// DescendGreaterThan calls the iterator for every item in the database within
// the range [last, pivot), until iterator returns false.
// DescendGreaterThan 遍历范围 [last, pivot)（不包含 pivot），直到 iterator 返回 false。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendGreaterThan(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, true, false, index, pivot, "", iterator)
}

// DescendLessOrEqual calls the iterator for every item in the database within
// the range [pivot, first], until iterator returns false.
// DescendLessOrEqual 遍历范围 [pivot, first]（包含 pivot），直到 iterator 返回 false。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendLessOrEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	return tx.scan(true, false, true, index, pivot, "", iterator)
}

// DescendRange calls the iterator for every item in the database within
// the range [lessOrEqual, greaterThan), until iterator returns false.
// DescendRange 遍历范围 [lessOrEqual, greaterThan)（包含 left，不包含 right），直到 iterator 返回 false。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendRange(index, lessOrEqual, greaterThan string,
	iterator func(key, value string) bool) error {
	return tx.scan(
		true, true, true, index, lessOrEqual, greaterThan, iterator,
	)
}

// AscendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// AscendEqual 遍历所有 value 等于 pivot 的项，直到 iterator 返回 false。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) AscendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var err error
	var less func(a, b string) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.AscendGreaterOrEqual(index, pivot, func(key, value string) bool {
		if less == nil {
			if key != pivot {
				return false
			}
		} else if less(pivot, value) {
			return false
		}
		return iterator(key, value)
	})
}

// DescendEqual calls the iterator for every item in the database that equals
// pivot, until iterator returns false.
// DescendEqual 遍历所有 value 等于 pivot 的项，直到 iterator 返回 false（降序语义）。
// When an index is provided, the results will be ordered by the item values
// 若指定 index，则按该 index 的 less() 对 value 排序。
// as specified by the less() function of the defined index.
// When an index is not provided, the results will be ordered by the item key.
// An invalid index will return an error.
func (tx *Tx) DescendEqual(index, pivot string,
	iterator func(key, value string) bool) error {
	var err error
	var less func(a, b string) bool
	if index != "" {
		less, err = tx.GetLess(index)
		if err != nil {
			return err
		}
	}
	return tx.DescendLessOrEqual(index, pivot, func(key, value string) bool {
		if less == nil {
			if key != pivot {
				return false
			}
		} else if less(value, pivot) {
			return false
		}
		return iterator(key, value)
	})
}

// rect is used by Intersects and Nearby
// rect 用于支持 Intersects 与 Nearby 的几何查询。
type rect struct {
	min, max []float64
}

func (r *rect) Rect(ctx interface{}) (min, max []float64) {
	return r.min, r.max
}

// Nearby searches for rectangle items that are nearby a target rect.
// Nearby 会在给定目标矩形附近寻找空间项，并按“从近到远”返回结果。
// All items belonging to the specified index will be returned in order of
// nearest to farthest.
// The specified index must have been created by AddIndex() and the target
// 指定的空间索引需要先通过 CreateSpatialIndex 创建，target 用 rect 字符串表示，
// is represented by the rect string. This string will be processed by the
// 并由 CreateSpatialIndex 传入的 bounds 函数解析。
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
// 无效索引将返回错误。
// The dist param is the distance of the bounding boxes. In the case of
// simple 2D points, it's the distance of the two 2D points squared.
// dist 表示边界框之间的距离；在简单 2D 点情况下，该距离是两点平方距离。
func (tx *Tx) Nearby(index, bounds string,
	iterator func(key, value string, dist float64) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		// 不能在 keys 树上做空间查询，直接返回 nil。
		return nil
	}
	// // wrap a rtree specific iterator around the user-defined iterator.
	// // 把 rtree 相关的 iterator 包装到用户自定义 iterator（该行原本被注释掉了）。
	iter := func(item rtred.Item, dist float64) bool {
		dbi := item.(*dbItem)
		return iterator(dbi.key, dbi.val, dist)
	}
	idx := tx.db.idxs[index]
	if idx == nil {
		// index was not found. return error
		// 索引不存在，返回 ErrNotFound。
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		// 该索引不是 r-tree 索引，直接返回 nil。
		return nil
	}
	// execute the nearby search
	// 执行最近邻（KNN）搜索。
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	// set the center param to false, which uses the box dist calc.
	// 将 center 设为 false，使用边界框距离计算。
	idx.rtr.KNN(&rect{min, max}, false, iter)
	return nil
}

// Intersects searches for rectangle items that intersect a target rect.
// Intersects 查找与目标矩形相交的空间项。
// The specified index must have been created by AddIndex() and the target
// 使用的空间索引需要先创建；target 由 rect 字符串表示，并由 bounds 函数解析。
// is represented by the rect string. This string will be processed by the
// same bounds function that was passed to the CreateSpatialIndex() function.
// An invalid index will return an error.
func (tx *Tx) Intersects(index, bounds string,
	iterator func(key, value string) bool) error {
	if tx.db == nil {
		return ErrTxClosed
	}
	if index == "" {
		// cannot search on keys tree. just return nil.
		// 不能在 keys 树上进行空间相交查询，直接返回 nil。
		return nil
	}
	// wrap a rtree specific iterator around the user-defined iterator.
	// 把 rtree 相关的 iterator 包装到用户自定义 iterator。
	iter := func(item rtred.Item) bool {
		dbi := item.(*dbItem)
		return iterator(dbi.key, dbi.val)
	}
	idx := tx.db.idxs[index]
	if idx == nil {
		// index was not found. return error
		// 索引不存在，返回 ErrNotFound。
		return ErrNotFound
	}
	if idx.rtr == nil {
		// not an r-tree index. just return nil
		// 该索引不是 r-tree 索引，直接返回 nil。
		return nil
	}
	// execute the search
	// 执行相交查询（Search）。
	var min, max []float64
	if idx.rect != nil {
		min, max = idx.rect(bounds)
	}
	idx.rtr.Search(&rect{min, max}, iter)
	return nil
}

// Len returns the number of items in the database
// Len 返回数据库中 item 的数量。
func (tx *Tx) Len() (int, error) {
	if tx.db == nil {
		return 0, ErrTxClosed
	}
	return tx.db.keys.Len(), nil
}

// IndexOptions provides an index with additional features or
// IndexOptions 允许为索引提供额外能力或替代行为。
// alternate functionality.
type IndexOptions struct {
	// CaseInsensitiveKeyMatching allow for case-insensitive
	// matching on keys when setting key/values.
	// CaseInsensitiveKeyMatching 允许在设置 key/value 时对 key 进行不区分大小写的匹配。
	CaseInsensitiveKeyMatching bool
}

// CreateIndex builds a new index and populates it with items.
// The items are ordered in an b-tree and can be retrieved using the
// Ascend* and Descend* methods.
// An error will occur if an index with the same name already exists.
//
// When a pattern is provided, the index will be populated with
// keys that match the specified pattern. This is a very simple pattern
// match where '*' matches on any number characters and '?' matches on
// any one character.
// The less function compares if string 'a' is less than string 'b'.
// It allows for indexes to create custom ordering. It's possible
// that the strings may be textual or binary. It's up to the provided
// less function to handle the content format and comparison.
// There are some default less function that can be used such as
// IndexString, IndexBinary, etc.
// 默认的 less 函数示例：IndexString、IndexBinary 等。
func (tx *Tx) CreateIndex(name, pattern string,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, nil)
}

// CreateIndexOptions is the same as CreateIndex except that it allows
// for additional options.
// CreateIndexOptions 允许传入额外的索引选项。
func (tx *Tx) CreateIndexOptions(name, pattern string,
	opts *IndexOptions,
	less ...func(a, b string) bool) error {
	return tx.createIndex(name, pattern, less, nil, opts)
}

// CreateSpatialIndex builds a new index and populates it with items.
// The items are organized in an r-tree and can be retrieved using the
// Intersects method.
// An error will occur if an index with the same name already exists.
//
// The rect function converts a string to a rectangle. The rectangle is
// represented by two arrays, min and max. Both arrays may have a length
// between 1 and 20, and both arrays must match in length. A length of 1 is a
// one dimensional rectangle, and a length of 4 is a four dimension rectangle.
// There is support for up to 20 dimensions.
// The values of min must be less than the values of max at the same dimension.
// Thus min[0] must be less-than-or-equal-to max[0].
// The IndexRect is a default function that can be used for the rect
// parameter.
// 参数 rect 由 IndexRect 这样的默认函数解析。
func (tx *Tx) CreateSpatialIndex(name, pattern string,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, nil)
}

// CreateSpatialIndexOptions is the same as CreateSpatialIndex except that
// it allows for additional options.
// CreateSpatialIndexOptions 允许传入额外的空间索引选项。
func (tx *Tx) CreateSpatialIndexOptions(name, pattern string,
	opts *IndexOptions,
	rect func(item string) (min, max []float64)) error {
	return tx.createIndex(name, pattern, nil, rect, nil)
}

// createIndex is called by CreateIndex() and CreateSpatialIndex()
// createIndex 是 CreateIndex() 与 CreateSpatialIndex() 的共同实现。
func (tx *Tx) createIndex(name string, pattern string,
	lessers []func(a, b string) bool,
	rect func(item string) (min, max []float64),
	opts *IndexOptions,
) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot create an index without a name.
		// an empty name index is designated for the main "keys" tree.
		// 没有名称无法创建索引；空名称索引对应主 keys 树。
		return ErrIndexExists
	}
	// check if an index with that name already exists.
	// 检查是否已存在同名索引。
	if _, ok := tx.db.idxs[name]; ok {
		// index with name already exists. error.
		// 同名索引已存在，返回错误。
		return ErrIndexExists
	}
	// genreate a less function
	// 生成 less 比较函数（用于索引排序）。
	var less func(a, b string) bool
	switch len(lessers) {
	default:
		// multiple less functions specified.
		// create a compound less function.
		// 多个 less 函数会组合成一个复合比较函数。
		less = func(a, b string) bool {
			for i := 0; i < len(lessers)-1; i++ {
				if lessers[i](a, b) {
					return true
				}
				if lessers[i](b, a) {
					return false
				}
			}
			return lessers[len(lessers)-1](a, b)
		}
	case 0:
		// no less function
		// 未提供 less 函数。
	case 1:
		less = lessers[0]
	}
	var sopts IndexOptions
	if opts != nil {
		sopts = *opts
	}
	if sopts.CaseInsensitiveKeyMatching {
		pattern = strings.ToLower(pattern)
	}
	// intialize new index
	// 初始化新的 index 实例。
	idx := &index{
		name:    name,
		pattern: pattern,
		less:    less,
		rect:    rect,
		db:      tx.db,
		opts:    sopts,
	}
	idx.rebuild()
	// save the index
	// 保存该索引到数据库中。
	tx.db.idxs[name] = idx
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		// 如果需要在回滚时恢复索引信息，则把索引记录到 rollback map。
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use nil to indicate that the index should be removed upon
			// rollback.
			// 使用 nil 表示回滚时移除该索引。
			tx.wc.rollbackIndexes[name] = nil
		}
	}
	return nil
}

// DropIndex removes an index.
// DropIndex 删除一个索引。
func (tx *Tx) DropIndex(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if tx.wc.itercount > 0 {
		return ErrTxIterating
	}
	if name == "" {
		// cannot drop the default "keys" index
		// 不能删除默认的 "keys" 索引。
		return ErrInvalidOperation
	}
	idx, ok := tx.db.idxs[name]
	if !ok {
		return ErrNotFound
	}
	// delete from the map.
	// this is all that is needed to delete an index.
	// 从索引映射表中删除即可完成索引删除。
	delete(tx.db.idxs, name)
	if tx.wc.rbkeys == nil {
		// store the index in the rollback map.
		// 把被删除的索引记录到回滚映射，以便必要时重建。
		if _, ok := tx.wc.rollbackIndexes[name]; !ok {
			// we use a non-nil copy of the index without the data to indicate
			// that the index should be rebuilt upon rollback.
			// 使用不含数据的 non-nil 索引副本，表示回滚时需要重建。
			tx.wc.rollbackIndexes[name] = idx.clearCopy()
		}
	}
	return nil
}

// Indexes returns a list of index names.
// Indexes 返回索引名称列表。
func (tx *Tx) Indexes() ([]string, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	names := make([]string, 0, len(tx.db.idxs))
	for name := range tx.db.idxs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// Rect is helper function that returns a string representation
// of a rect. IndexRect() is the reverse function and can be used
// Rect 把矩形（min/max）转成字符串；IndexRect 是反向解析函数，可从字符串生成矩形。
// to generate a rect from a string.
func Rect(min, max []float64) string {
	r := grect.Rect{Min: min, Max: max}
	return r.String()
}

// Point is a helper function that converts a series of float64s
// to a rectangle for a spatial index.
// Point 把一组 float64 坐标转换为适用于空间索引的矩形（min=max）。
func Point(coords ...float64) string {
	return Rect(coords, coords)
}

// IndexRect is a helper function that converts string to a rect.
// Rect() is the reverse function and can be used to generate a string
// IndexRect 把字符串解析成矩形；Rect 则是反向函数，可把矩形转回字符串。
// from a rect.
func IndexRect(a string) (min, max []float64) {
	r := grect.Get(a)
	return r.Min, r.Max
}

// IndexString is a helper function that return true if 'a' is less than 'b'.
// IndexString 提供不区分大小写的字符串比较：判断 a < b。
// This is a case-insensitive comparison. Use the IndexBinary() for comparing
// 对区分大小写的比较请使用 IndexBinary()。
// case-sensitive strings.
func IndexString(a, b string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] >= 'A' && a[i] <= 'Z' {
			if b[i] >= 'A' && b[i] <= 'Z' {
				// both are uppercase, do nothing
				// 两个字符都是大写，不需要转换。
				if a[i] < b[i] {
					return true
				} else if a[i] > b[i] {
					return false
				}
			} else {
				// a is uppercase, convert a to lowercase
				// a 是大写，把 a 转成小写再比较。
				if a[i]+32 < b[i] {
					return true
				} else if a[i]+32 > b[i] {
					return false
				}
			}
		} else if b[i] >= 'A' && b[i] <= 'Z' {
			// b is uppercase, convert b to lowercase
			// b 是大写，把 b 转成小写再比较。
			if a[i] < b[i]+32 {
				return true
			} else if a[i] > b[i]+32 {
				return false
			}
		} else {
			// neither are uppercase
			// 两者都不是大写，直接按原始字节比较。
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
	}
	return len(a) < len(b)
}

// IndexBinary is a helper function that returns true if 'a' is less than 'b'.
// IndexBinary 直接比较字符串的原始字节序（区分大小写）。
// This compares the raw binary of the string.
func IndexBinary(a, b string) bool {
	return a < b
}

// IndexInt is a helper function that returns true if 'a' is less than 'b'.
// IndexInt 会把字符串解析为 int64 后进行比较。
func IndexInt(a, b string) bool {
	ia, _ := strconv.ParseInt(a, 10, 64)
	ib, _ := strconv.ParseInt(b, 10, 64)
	return ia < ib
}

// IndexUint is a helper function that returns true if 'a' is less than 'b'.
// IndexUint 会把字符串解析为 uint64 后进行比较（适用于 Uint() 转换后写入的数据）。
// This compares uint64s that are added to the database using the
// Uint() conversion function.
func IndexUint(a, b string) bool {
	ia, _ := strconv.ParseUint(a, 10, 64)
	ib, _ := strconv.ParseUint(b, 10, 64)
	return ia < ib
}

// IndexFloat is a helper function that returns true if 'a' is less than 'b'.
// IndexFloat 会把字符串解析为 float64 后进行比较（适用于 Float() 转换后写入的数据）。
// This compares float64s that are added to the database using the
// Float() conversion function.
func IndexFloat(a, b string) bool {
	ia, _ := strconv.ParseFloat(a, 64)
	ib, _ := strconv.ParseFloat(b, 64)
	return ia < ib
}

// IndexJSON provides for the ability to create an index on any JSON field.
// IndexJSON 支持在任意 JSON 字段上创建索引；当字段为字符串时比较不区分大小写。
// When the field is a string, the comparison will be case-insensitive.
// It returns a helper function used by CreateIndex.
func IndexJSON(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), false)
	}
}

// IndexJSONCaseSensitive provides for the ability to create an index on
// any JSON field.
// IndexJSONCaseSensitive 支持在任意 JSON 字段上创建索引；当字段为字符串时比较区分大小写。
// When the field is a string, the comparison will be case-sensitive.
// It returns a helper function used by CreateIndex.
func IndexJSONCaseSensitive(path string) func(a, b string) bool {
	return func(a, b string) bool {
		return gjson.Get(a, path).Less(gjson.Get(b, path), true)
	}
}

// Desc is a helper function that changes the order of an index.
// Desc 用于反转索引的排序方向。
func Desc(less func(a, b string) bool) func(a, b string) bool {
	return func(a, b string) bool { return less(b, a) }
}

//// Wrappers around btree Ascend/Descend
// 以下是对 btree Ascend/Descend 的包装函数。

func bLT(tr *btree.BTree, a, b interface{}) bool { return tr.Less(a, b) }
func bGT(tr *btree.BTree, a, b interface{}) bool { return tr.Less(b, a) }

// func bLTE(tr *btree.BTree, a, b interface{}) bool { return !tr.Less(b, a) }
// func bGTE(tr *btree.BTree, a, b interface{}) bool { return !tr.Less(a, b) }
// 这两个函数被注释掉了，分别用于 <= 与 >= 的比较封装。

// Ascend
// 升序遍历封装。

func btreeAscend(tr *btree.BTree, iter func(item interface{}) bool) {
	tr.Ascend(nil, iter)
}

func btreeAscendLessThan(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(nil, func(item interface{}) bool {
		return bLT(tr, item, pivot) && iter(item)
	})
}

func btreeAscendGreaterOrEqual(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(pivot, iter)
}

func btreeAscendRange(tr *btree.BTree, greaterOrEqual, lessThan interface{},
	iter func(item interface{}) bool,
) {
	tr.Ascend(greaterOrEqual, func(item interface{}) bool {
		return bLT(tr, item, lessThan) && iter(item)
	})
}

// Descend
// 降序遍历封装。

func btreeDescend(tr *btree.BTree, iter func(item interface{}) bool) {
	tr.Descend(nil, iter)
}

func btreeDescendGreaterThan(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(nil, func(item interface{}) bool {
		return bGT(tr, item, pivot) && iter(item)
	})
}

func btreeDescendRange(tr *btree.BTree, lessOrEqual, greaterThan interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(lessOrEqual, func(item interface{}) bool {
		return bGT(tr, item, greaterThan) && iter(item)
	})
}

func btreeDescendLessOrEqual(tr *btree.BTree, pivot interface{},
	iter func(item interface{}) bool,
) {
	tr.Descend(pivot, iter)
}

func btreeNew(less func(a, b interface{}) bool) *btree.BTree {
	// Using NewNonConcurrent because we're managing our own locks.
	// 使用 NewNonConcurrent 是因为该实现会自行管理锁。
	return btree.NewNonConcurrent(less)
}
