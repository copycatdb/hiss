class Record:
    """asyncpg-style Record with dict-like and index access."""
    __slots__ = ("_keys", "_values", "_map")

    def __init__(self, keys: list, values: list):
        self._keys = keys
        self._values = values
        self._map = {k: i for i, k in enumerate(keys)}

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return self._values[self._map[key]]

    def __contains__(self, key):
        return key in self._map

    def __len__(self):
        return len(self._values)

    def __repr__(self):
        items = ", ".join(f"{k}={v!r}" for k, v in zip(self._keys, self._values))
        return f"<Record {items}>"

    def __eq__(self, other):
        if isinstance(other, Record):
            return self._keys == other._keys and self._values == other._values
        return NotImplemented

    def keys(self):
        return list(self._keys)

    def values(self):
        return list(self._values)

    def items(self):
        return list(zip(self._keys, self._values))

    def get(self, key, default=None):
        try:
            return self[key]
        except (KeyError, IndexError):
            return default
