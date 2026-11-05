;; `ticker N`: prints "tick" once a second for N seconds (wall clock: run
;; it with `deterministic = false`), then writes "pumasi dashboard ok" to
;; /output/result.txt and prints "done". The dashboard's end-to-end tests
;; submit it; ticker.wasm is this file compiled (a unit test checks).
(module
  (import "wasi_snapshot_preview1" "args_sizes_get" (func $sizes (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "args_get" (func $args_get (param i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "fd_write" (func $write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "clock_time_get" (func $now (param i32 i64 i32) (result i32)))
  (import "wasi_snapshot_preview1" "path_open" (func $open (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
  (memory (export "memory") 1)
  (data (i32.const 8192) "tick\n")
  (data (i32.const 8208) "done\n")
  (data (i32.const 8224) "result.txt")
  (data (i32.const 8240) "pumasi dashboard ok\n")
  (func $mono (result i64)
    (drop (call $now (i32.const 1) (i64.const 1000) (i32.const 48)))
    (i64.load (i32.const 48)))
  (func $print (param $fd i32) (param $p i32) (param $n i32) (result i32)
    (i32.store (i32.const 8) (local.get $p))
    (i32.store (i32.const 12) (local.get $n))
    (call $write (local.get $fd) (i32.const 8) (i32.const 1) (i32.const 16)))
  (func (export "_start")
    (local $p i32) (local $c i32) (local $n i32) (local $i i32) (local $start i64)
    (drop (call $sizes (i32.const 0) (i32.const 4)))
    (drop (call $args_get (i32.const 64) (i32.const 1024)))
    (if (i32.ge_u (i32.load (i32.const 0)) (i32.const 2))
      (then
        (local.set $p (i32.load (i32.const 68)))
        (block $d (loop $l
          (local.set $c (i32.load8_u (local.get $p)))
          (br_if $d (i32.lt_u (local.get $c) (i32.const 48)))
          (br_if $d (i32.gt_u (local.get $c) (i32.const 57)))
          (local.set $n (i32.add (i32.mul (local.get $n) (i32.const 10))
                                 (i32.sub (local.get $c) (i32.const 48))))
          (local.set $p (i32.add (local.get $p) (i32.const 1)))
          (br $l)))))
    (block $done (loop $tick
      (br_if $done (i32.ge_u (local.get $i) (local.get $n)))
      (local.set $start (call $mono))
      (block $w (loop $spin
        (br_if $w (i64.ge_u (i64.sub (call $mono) (local.get $start)) (i64.const 1000000000)))
        (br $spin)))
      (drop (call $print (i32.const 1) (i32.const 8192) (i32.const 5)))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br $tick)))
    ;; /output is preopened as fd 4, after /input (fd 3).
    (if (call $open (i32.const 4) (i32.const 0) (i32.const 8224) (i32.const 10)
          (i32.const 9) (i64.const 536870911) (i64.const 536870911) (i32.const 0) (i32.const 40))
      (then (call $exit (i32.const 11))))
    (drop (call $print (i32.load (i32.const 40)) (i32.const 8240) (i32.const 20)))
    (drop (call $print (i32.const 1) (i32.const 8208) (i32.const 5)))
    (call $exit (i32.const 0))))
