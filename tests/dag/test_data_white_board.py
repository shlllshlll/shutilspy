from shutils.dag.data_white_board import DataWhiteBoardMixin


class TestSyncDataWhiteBoard:
    def test_set_and_get(self):
        board = DataWhiteBoardMixin()
        board.sync_white_board["key1"] = "value1"
        assert board.sync_white_board["key1"] == "value1"

    def test_contains(self):
        board = DataWhiteBoardMixin()
        board.sync_white_board["key1"] = "value1"
        assert "key1" in board.sync_white_board
        assert "key2" not in board.sync_white_board

    def test_len(self):
        board = DataWhiteBoardMixin()
        assert len(board.sync_white_board) == 0
        board.sync_white_board["key1"] = "value1"
        assert len(board.sync_white_board) == 1

    def test_del(self):
        board = DataWhiteBoardMixin()
        board.sync_white_board["key1"] = "value1"
        del board.sync_white_board["key1"]
        assert "key1" not in board.sync_white_board

    def test_set_data(self):
        board = DataWhiteBoardMixin()
        board.sync_white_board.set_data(a=1, b=2)
        assert board.sync_white_board["a"] == 1
        assert board.sync_white_board["b"] == 2

    def test_get_with_default(self):
        board = DataWhiteBoardMixin()
        assert board.sync_white_board.get("missing", "default") == "default"

    def test_keys_values_items(self):
        board = DataWhiteBoardMixin()
        board.sync_white_board["a"] = 1
        board.sync_white_board["b"] = 2
        assert set(board.sync_white_board.keys()) == {"a", "b"}
        assert set(board.sync_white_board.values()) == {1, 2}
        assert set(board.sync_white_board.items()) == {("a", 1), ("b", 2)}

    def test_bool(self):
        board = DataWhiteBoardMixin()
        assert not board.sync_white_board
        board.sync_white_board["key"] = "val"
        assert board.sync_white_board

    def test_copy(self):
        board1 = DataWhiteBoardMixin()
        board1.sync_white_board["key"] = "value"
        board2 = DataWhiteBoardMixin()
        board1.sync_white_board.copy(board2)
        assert board2.sync_white_board["key"] == "value"

    def test_copy_deep(self):
        board1 = DataWhiteBoardMixin()
        board1.sync_white_board["key"] = [1, 2, 3]
        board2 = DataWhiteBoardMixin()
        board1.sync_white_board.copy(board2, deep_copy=True)
        board2.sync_white_board["key"].append(4)
        assert board1.sync_white_board["key"] == [1, 2, 3]  # Original unchanged


class TestAsyncDataWhiteBoard:
    async def test_set_and_get(self):
        board = DataWhiteBoardMixin()
        await board.async_white_board.set_item("key1", "value1")
        result = await board.async_white_board.get_item("key1")
        assert result == "value1"

    async def test_contains(self):
        board = DataWhiteBoardMixin()
        await board.async_white_board.set_item("key1", "value1")
        assert await board.async_white_board.contains("key1")
        assert not await board.async_white_board.contains("key2")

    async def test_len(self):
        board = DataWhiteBoardMixin()
        assert await board.async_white_board.len() == 0
        await board.async_white_board.set_item("key1", "value1")
        assert await board.async_white_board.len() == 1

    async def test_del(self):
        board = DataWhiteBoardMixin()
        await board.async_white_board.set_item("key1", "value1")
        await board.async_white_board.del_item("key1")
        assert not await board.async_white_board.contains("key1")

    async def test_rlock_wlock(self):
        board = DataWhiteBoardMixin()
        async with board.async_white_board.rlock():
            pass
        async with board.async_white_board.wlock():
            pass


class TestDataWhiteBoardMixin:
    def test_repr(self):
        board = DataWhiteBoardMixin()
        assert "DataWhiteBoard" in repr(board)
