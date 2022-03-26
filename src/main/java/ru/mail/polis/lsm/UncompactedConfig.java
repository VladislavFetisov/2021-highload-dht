package ru.mail.polis.lsm;

import java.nio.file.Path;

public class UncompactedConfig extends DAOConfig {
    public UncompactedConfig(Path dir) {
        super(dir, DEFAULT_MEMORY_LIMIT, Integer.MAX_VALUE);
    }
}
