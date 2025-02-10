/*
 * The MIT License (MIT) Copyright (c) 2020-2023 artipie.com
 * https://github.com/artipie/artipie/blob/master/LICENSE.txt
 */
package com.artipie.asto.cleanup;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Cleanup report for storage supporting it
 */
public class CleanupReport {
    private int totalCleaned;
    private int nbUnused;
    private int nbAged;

    public int getTotalCleaned() {
        return totalCleaned;
    }

    public void setTotalCleaned(int totalCleaned) {
        this.totalCleaned = totalCleaned;
    }

    public int getNbUnused() {
        return nbUnused;
    }

    public void setNbUnused(int nbUnused) {
        this.nbUnused = nbUnused;
    }

    public int getNbAged() {
        return nbAged;
    }

    public void setNbAged(int nbAged) {
        this.nbAged = nbAged;
    }
}
