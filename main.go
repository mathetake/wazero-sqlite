package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

var ctx = context.Background()

// sqlite3Wasm is the Wasm binary compiled from the SQLite source code.
// https://github.com/fluencelabs/sqlite/releases/tag/v0.16.0_w
//
//go:embed sqlite3.wasm
var sqlite3Wasm []byte

func main() {
	// Create a wazero runtime.
	r := wazero.NewRuntime(ctx)
	defer r.Close(ctx)

	// Initializes WASI (WebAssembly System Interface) environment.
	_, err := wasi_snapshot_preview1.Instantiate(ctx, r)
	if err != nil {
		log.Panicln(err)
	}

	// Compile sqlite Wasm binary.
	compiledSqlite, err := r.CompileModule(ctx, sqlite3Wasm)
	if err != nil {
		log.Panicln(err)
	}

	s := newSqlModule(r, compiledSqlite)

	// Create table.
	s.execSql(`CREATE TABLE users (id int, name varchar(10))`)

	// Insert values.
	s.execSql(`INSERT INTO users(id, name) VALUES(0, 'go'), (1, 'zig'), (2, 'whatever')`)

	// Select users!
	users := s.execSelectUsers("SELECT id, name FROM users")

	for _, user := range users {
		fmt.Printf("user: id=%d, name='%s'\n", user.id, user.name)
	}
}

// sqliteModule corresponds to a Wasm module instance used to execute queries against the in-Wasm-memory db.
type sqliteModule struct {
	// memory holds the memory instance of this module.
	memory api.Memory
	// open holds the function for "sqlite3_open_v2" in SQLite C interface.
	open api.Function
	// exec holds the function for "sqlite3_exec" in SQLite C interface.
	exec api.Function
	// getResultPtr holds the function for "sqlite3_exec" in SQLite C interface.
	getResultPtr api.Function
	// getResultSize holds the function for "sqlite3_exec" in SQLite C interface.
	getResultSize api.Function
	// prepare holds the function for "sqlite3_prepare_v2" in SQLite C interface.
	prepare api.Function
	// step holds the function for "sqlite3_exec" in SQLite C interface.
	step api.Function
	// columnInt holds the function for "sqlite3_exec" in SQLite C interface.
	columnInt api.Function
	// columnText holds the function for "sqlite3_exec" in SQLite C interface.
	columnText api.Function
	alloc      api.Function
	// dbHandle is the identifier assigned to an opened database.
	dbHandle uint64
}

// newSqlModule creates a new sqliteModule in the given wazero.Runtime `r`.
func newSqlModule(r wazero.Runtime, compiledSqlite wazero.CompiledModule) *sqliteModule {
	sqlite, err := r.InstantiateModule(ctx, compiledSqlite, wazero.NewModuleConfig())
	if err != nil {
		log.Panicln(err)
	}

	s := &sqliteModule{
		memory:        sqlite.Memory(),
		open:          sqlite.ExportedFunction("sqlite3_open_v2"),
		exec:          sqlite.ExportedFunction("sqlite3_exec"),
		getResultPtr:  sqlite.ExportedFunction("get_result_ptr"),
		getResultSize: sqlite.ExportedFunction("get_result_size"),
		alloc:         sqlite.ExportedFunction("allocate"),
		prepare:       sqlite.ExportedFunction("sqlite3_prepare_v2"),
		step:          sqlite.ExportedFunction("sqlite3_step"),
		columnInt:     sqlite.ExportedFunction("sqlite3_column_int64"),
		columnText:    sqlite.ExportedFunction("sqlite3_column_text"),
	}

	dbNamePtr, dbNameSize := s.allocateString(":memory:")
	fsNamePTr, fsNameSize := s.allocateString("")

	// Create the db.
	_, err = s.open.Call(ctx, dbNamePtr, dbNameSize, 0b110, fsNamePTr, fsNameSize)
	if err != nil {
		log.Panicln(err)
	}

	// Get the db handle.
	res, err := s.getResultPtr.Call(ctx)
	if err != nil {
		log.Panicln(err)
	}
	s.ensureStatusCodeSuccess(uint32(res[0]), "db pointer")

	dbHandle, ok := s.memory.ReadUint32Le(ctx, uint32(res[0]+4))
	if !ok {
		log.Panicln("cannot take db pointer")
	}
	s.dbHandle = uint64(dbHandle)
	return s
}

type user struct {
	id   int
	name string
}

func (s *sqliteModule) execSelectUsers(query string) (users []*user) {
	// Create prepared statement!
	queryPtr, querySize := s.allocateString(query)

	// Get the prepared statement for the query.
	_, err := s.prepare.Call(ctx, s.dbHandle, queryPtr, querySize)
	if err != nil {
		log.Panicf("failed to call prepare query %s: %v", query, err)
	}

	res, err := s.getResultPtr.Call(ctx)
	if err != nil {
		log.Panicf("error getting result ptr: %v", err)
	}
	s.ensureStatusCodeSuccess(uint32(res[0]), "failed to prepare")

	// Read the prepared statement's pointer.
	stmt, ok := s.memory.ReadUint32Le(ctx, uint32(res[0]+4))
	if !ok || stmt == 0 {
		log.Panicf("failed to read prepared statement at %d", res[0]+4)
	}

	// Start retrieving each column.
	rc := s.execStep(stmt)
	for rc == SQLITE_ROW { // Continue as long as we see ROW.
		// id = int on 0-th column.
		id := s.readInt(stmt, 0)
		// name = text on 1-th column.
		name := s.readText(stmt, 1)

		users = append(users, &user{id: id, name: name})

		// Advance the step.
		rc = s.execStep(stmt)
	}
	return
}

const SQLITE_ROW = 100

// readInt tries to read the integer column in the stmt.
func (s *sqliteModule) readInt(stmt uint32, columnIndex uint32) int {
	res, err := s.columnInt.Call(ctx, uint64(stmt), uint64(columnIndex))
	if err != nil {
		log.Panicf("failed to read %d-th column as integer: %v", columnIndex, err)
	}
	return int(res[0])
}

// readText tries to read the text column in the stmt.
func (s *sqliteModule) readText(stmt uint32, columnIndex uint32) string {
	_, err := s.columnText.Call(ctx, uint64(stmt), uint64(columnIndex))
	if err != nil {
		log.Panicf("failed to read %d-th column as text: %v", columnIndex, err)
	}

	textPtr, err := s.getResultPtr.Call(ctx)
	if err != nil {
		log.Panicf("failed to get %d-th column text ptr: %v", columnIndex, err)
	}

	textSize, err := s.getResultSize.Call(ctx)
	if err != nil {
		log.Panicf("failed to get %d-th column text size: %v", columnIndex, err)
	}

	ptr, size := uint32(textPtr[0]), uint32(textSize[0])
	raw, ok := s.memory.Read(ctx, ptr, size)
	if !ok {
		log.Panicf("failed to read text(size=%d) at %d", ptr, size)
	}
	return string(raw)
}

func (s *sqliteModule) execStep(stmt uint32) int {
	res, err := s.step.Call(ctx, uint64(stmt))
	if err != nil {
		log.Panicf("failed to call step: %v", err)
	}
	return int(res[0])
}

func (s *sqliteModule) allocateString(str string) (ptr, size uint64) {
	res, err := s.alloc.Call(ctx, uint64(len(str)), 0)
	if err != nil {
		log.Panicln(err)
	}

	ptr = res[0]

	if ok := s.memory.Write(ctx, uint32(res[0]), []byte(str)); !ok {
		log.Panicln("failed to write name")
	}
	return ptr, uint64(len(str))
}

func (s *sqliteModule) execSql(query string) {
	queryPtr, querySize := s.allocateString(query)

	// Execute query.
	_, err := s.exec.Call(ctx, s.dbHandle, queryPtr, querySize, 0, 0)
	if err != nil {
		log.Panicf("error execution query '%s': %v", query, err)
	}

	res, err := s.getResultPtr.Call(ctx)
	if err != nil {
		log.Panicf("error getting result ptr: %v", err)
	}

	errMsgPtr, ok := s.memory.ReadUint32Le(ctx, uint32(res[0]+4))
	if !ok {
		log.Panicln("cannot read err msg ptr")
	}

	errMsgSize, ok := s.memory.ReadUint32Le(ctx, uint32(res[0]+8))
	if !ok {
		log.Panicln("cannot read err msg size")
	}

	var errMsg string
	if errMsgSize != 0 {
		raw, ok := s.memory.Read(ctx, errMsgPtr, errMsgSize)
		if !ok {
			log.Panicln("cannot read err msg")
		}
		errMsg = string(raw)
	}
	s.ensureStatusCodeSuccess(uint32(res[0]), errMsg)
}

func (s *sqliteModule) ensureStatusCodeSuccess(resultPpr uint32, errMsg string) {
	retCode, ok := s.memory.ReadUint32Le(ctx, resultPpr)
	if !ok {
		log.Panicln("cannot read return code")
	}

	if retCode != 0 {
		log.Panicf("got error status %d != 0\ndetail: %s", retCode, errMsg)
	}
}
