package raft

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fn := filepath.Join(dir, "raft.bin")
	p := NewFilePersister(fn)

	data := &PersistentData{
		Version:     PersistentDataVersion,
		CurrentTerm: 7,
		VotedFor:    3,
		Log: []LogEntry{
			{Index: 0, Term: 0},
			{Index: 1, Term: 1, Cmd: Command{Op: OpPut, Key: "k", Value: "中文 value", ClientID: 42, Seq: 9}},
			{Index: 2, Term: 2, Cmd: Command{Op: OpDelete, Key: "k", ClientID: 42, Seq: 10}},
		},
	}
	if err := p.Save(data); err != nil {
		t.Fatal(err)
	}
	got, err := p.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentTerm != 7 || got.VotedFor != 3 || len(got.Log) != 3 {
		t.Fatalf("恢复数据不符: %+v", got)
	}
	if got.Log[1].Cmd.Key != "k" || got.Log[1].Cmd.Value != "中文 value" || got.Log[1].Cmd.Seq != 9 {
		t.Fatalf("日志条目恢复错误: %+v", got.Log[1].Cmd)
	}
	t.Logf("判定依据: FilePersister 保存 term=%d votedFor=%d %d 条日志后原样恢复（含 UTF-8 值）",
		got.CurrentTerm, got.VotedFor, len(got.Log))

	// 文件必须是原子写入后的完整形态。
	raw, err := os.ReadFile(fn)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw[:4]) != "RAFT" {
		t.Fatal("magic 不正确")
	}
}

func TestPersisterVersionCompat(t *testing.T) {
	dir := t.TempDir()

	t.Run("future version rejected", func(t *testing.T) {
		raw := buildRawVersion(999, 1, -1)
		fn := filepath.Join(dir, "future.bin")
		if err := os.WriteFile(fn, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := NewFilePersister(fn).Load()
		re, ok := err.(*Error)
		if !ok || re.Kind != ErrUnsupportedVersionKind {
			t.Fatalf("期望 ErrUnsupportedVersionKind，得到 %T %v", err, err)
		}
		t.Logf("判定依据: 未来版本 v999 被拒绝: %v", err)
	})

	t.Run("v1 accepted", func(t *testing.T) {
		raw := buildRawVersion(1, 5, -1)
		fn := filepath.Join(dir, "v1.bin")
		if err := os.WriteFile(fn, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		got, err := NewFilePersister(fn).Load()
		if err != nil {
			t.Fatal(err)
		}
		if got.Version != 1 || got.CurrentTerm != 5 {
			t.Fatalf("v1 恢复错误: %+v", got)
		}
		t.Log("判定依据: 当前支持的 v1 文件正常读取（向后兼容）")
	})

	t.Run("corruption detected", func(t *testing.T) {
		raw := buildRawVersion(1, 5, -1)
		raw[len(raw)-1] ^= 0xff // 破坏末尾字节
		fn := filepath.Join(dir, "corrupt.bin")
		if err := os.WriteFile(fn, raw, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := NewFilePersister(fn).Load()
		if err == nil || !strings.Contains(err.Error(), "checksum") {
			t.Fatalf("期望 CRC 校验失败，得到 %v", err)
		}
		t.Logf("判定依据: 损坏文件被 CRC32 检出: %v", err)
	})

	t.Run("missing file means fresh state", func(t *testing.T) {
		got, err := NewFilePersister(filepath.Join(dir, "nonexistent.bin")).Load()
		if err != nil {
			t.Fatal(err)
		}
		if got.CurrentTerm != 0 || got.VotedFor != -1 {
			t.Fatalf("期望全新状态，得到 %+v", got)
		}
		t.Log("判定依据: 文件缺失视为全新节点 term=0 votedFor=-1")
	})
}
