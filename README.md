# MigMaster Roll — Sequential WebSocket Roll

ROLL memproses WebSocket satu per satu secara berurutan:

**WebSocket 1:** LOGIN → ENTER ROOM → LEAVE ROOM → LOGOUT  
**WebSocket 2:** LOGIN → ENTER ROOM → LEAVE ROOM → LOGOUT  
… sampai WebSocket terakhir, lalu kembali ke WebSocket 1 dan mengulang selama ROLL aktif.

Semua koneksi tetap memakai 1 WebSocket per akun. Tidak ada login 10 WebSocket sekaligus saat ROLL berjalan.

`delay` di frontend mengatur jeda antar langkah ROLL.

## Start

```bash
npm install
npm start
```
