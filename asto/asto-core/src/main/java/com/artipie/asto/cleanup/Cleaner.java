/*
 * The MIT License (MIT) Copyright (c) 2020-2023 artipie.com
 * https://github.com/artipie/artipie/blob/master/LICENSE.txt
 */
package com.artipie.asto.cleanup;

import com.artipie.asto.Key;
import com.artipie.asto.Meta;
import com.artipie.asto.Storage;
import com.jcabi.log.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Cleaner implements Subscriber<Key> {

    private final CompletableFuture<CleanupReport> reportFuture;
    private final CleanupPolicy policy;
    private final CleanupReport report;
    private final Storage storage;
    private final Instant startupInstant;

    public Cleaner(Storage storage, CleanupPolicy policy) {
        this.storage = storage;
        this.policy = policy;
        this.report = new CleanupReport();
        this.reportFuture = new CompletableFuture<>();
        this.startupInstant = Instant.now();
    }
    
    public static CompletableFuture<CleanupReport> cleanup(Storage storage, CleanupPolicy policy) {
        var cleaner = new Cleaner(storage, policy);
        storage.walk().subscribe(cleaner);
        return cleaner.reportFuture;
    }

    @Override
    public void onSubscribe(Subscription s) {
        Logger.info(this, "Start cleaning storage %s", this.storage.identifier());
    }

    @Override
    public void onNext(Key key) {
        try {
            var meta = storage.metadata(key).get();
            boolean shouldDelete = false;

            var updatedAt = meta.read(Meta.OP_UPDATED_AT);
            if(updatedAt.isPresent()) {
                var age = Duration.between(updatedAt.get(), this.startupInstant);
                if(age.compareTo(policy.getMaxAge()) > 0) {
                    report.setNbAged(report.getNbAged()+1);
                    shouldDelete = true;
                }
            }

            var accessedAt = meta.read(Meta.OP_ACCESSED_AT);
            if(accessedAt.isPresent()) {
                var unused = Duration.between(accessedAt.get(), this.startupInstant);
                if (unused.compareTo(policy.getMaxUnused()) > 0) {
                    report.setNbUnused(report.getNbUnused() + 1);
                    shouldDelete = true;
                }
            }

            if(shouldDelete) {
                Logger.trace(this, "Cleaning %s from storage %s", key, this.storage.identifier());
                storage.delete(key).join();
                report.setTotalCleaned(report.getTotalCleaned()+1);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onError(Throwable t) {
        Logger.error(this, "Error cleaning storage %s : %[exception]s", this.storage.identifier(), t);
    }

    @Override
    public void onComplete() {
        Logger.info(this, "Stop cleaning storage %s : %s", this.storage.identifier(), this.report);
        this.reportFuture.complete(this.report);
    }

}
