/*
 * The MIT License (MIT) Copyright (c) 2020-2023 artipie.com
 * https://github.com/artipie/artipie/blob/master/LICENSE.txt
 */
package com.artipie.asto.cleanup;

import java.time.Duration;

/**
 * Cleanup policy for storage supporting it
 */
public class CleanupPolicy {

    private Duration maxUnused;

    public Duration getMaxUnused() {
        return maxUnused;
    }

    public void setMaxUnused(Duration maxUnused) {
        this.maxUnused = maxUnused;
    }

    private Duration maxAge;

    public void setMaxAge(Duration maxAge) {
        this.maxAge = maxAge;
    }

    public Duration getMaxAge() {
        return maxAge;
    }
}
