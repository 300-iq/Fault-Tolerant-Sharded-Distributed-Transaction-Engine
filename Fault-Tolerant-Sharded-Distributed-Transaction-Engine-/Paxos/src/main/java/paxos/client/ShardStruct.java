package paxos.client;

final class ShardStruct {

    private volatile int[] layoutOverride;

    int clusterForAccount(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            return 0;
        }
        int id;
        try {
            id = Integer.parseInt(accountId.trim());
        } catch (Exception e) {
            return 0;
        }

        int[] override = layoutOverride;
        if (override != null && id >= 1 && id < override.length) {
            int c = override[id];
            if (c >= 1 && c <= 3) {
                return c;
            }
        }

        if (id >= 1 && id <= 3000) {
            return 1;
        }
        if (id >= 3001 && id <= 6000) {
            return 2;
        }
        if (id >= 6001 && id <= 9000) {
            return 3;
        }
        return 0;
    }

    void applyShardLayout(int[] mapping) {
        if (mapping == null) {
            layoutOverride = null;
            return;
        }
        int[] copy = java.util.Arrays.copyOf(mapping, mapping.length);
        layoutOverride = copy;
    }
}
