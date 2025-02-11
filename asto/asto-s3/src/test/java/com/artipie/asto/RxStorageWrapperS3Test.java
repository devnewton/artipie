/*
 * The MIT License (MIT) Copyright (c) 2020-2023 artipie.com
 * https://github.com/artipie/artipie/blob/master/LICENSE.txt
 */
package com.artipie.asto;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.amazonaws.services.s3.AmazonS3;
import com.amihaiemil.eoyaml.Yaml;
import com.artipie.asto.blocking.BlockingStorage;
import com.artipie.asto.cleanup.Cleaner;
import com.artipie.asto.cleanup.CleanupPolicy;
import com.artipie.asto.ext.ContentAs;
import com.artipie.asto.factory.Config;
import com.artipie.asto.factory.StoragesLoader;
import com.artipie.asto.rx.RxStorage;
import com.artipie.asto.rx.RxStorageWrapper;
import hu.akarnokd.rxjava2.interop.SingleInterop;
import io.reactivex.Single;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsEqual;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link RxStorageWrapper}.
 */
final class RxStorageWrapperS3Test {

    /**
     * Mock S3 server.
     */
    @RegisterExtension
    static final S3MockExtension MOCK = S3MockExtension.builder()
        .withSecureConnection(false)
        .build();

    /**
     * Bucket to use in tests.
     */
    private String bucket;

    /**
     * Original storage.
     */
    private Storage original;

    /**
     * Reactive wrapper of original storage.
     */
    private RxStorageWrapper wrapper;

    @BeforeEach
    void setUp(final AmazonS3 client) {
        this.bucket = UUID.randomUUID().toString();
        client.createBucket(this.bucket);
        this.original = StoragesLoader.STORAGES
            .newObject(
                "s3",
                new Config.YamlStorageConfig(
                    Yaml.createYamlMappingBuilder()
                        .add("accessedAt", "true")
                        .add("region", "us-east-1")
                        .add("bucket", this.bucket)
                        .add("endpoint", String.format("http://localhost:%d", MOCK.getHttpPort()))
                        .add(
                            "credentials",
                            Yaml.createYamlMappingBuilder()
                                .add("type", "basic")
                                .add("accessKeyId", "foo")
                                .add("secretAccessKey", "bar")
                                .build()
                        )
                        .build()
                )
            );
        this.wrapper = new RxStorageWrapper(this.original);
    }

    @Test
    void checksExistence() {
        final Key key = new Key.From("a");
        this.original.save(key, Content.EMPTY).join();
        MatcherAssert.assertThat(
            this.wrapper.exists(key).blockingGet(),
            new IsEqual<>(true)
        );
    }

    @Test
    void listsItemsByPrefix() {
        this.original.save(new Key.From("a/b/c"), Content.EMPTY).join();
        this.original.save(new Key.From("a/d"), Content.EMPTY).join();
        this.original.save(new Key.From("z"), Content.EMPTY).join();
        final Collection<String> keys = this.wrapper.list(new Key.From("a"))
            .blockingGet()
            .stream()
            .map(Key::string)
            .collect(Collectors.toList());
        MatcherAssert.assertThat(
            keys,
            new IsEqual<>(Arrays.asList("a/b/c", "a/d"))
        );
    }

    @Test
    void savesItems() {
        this.wrapper.save(
            new Key.From("foo/file1"), Content.EMPTY
        ).blockingAwait();
        this.wrapper.save(
            new Key.From("file2"), Content.EMPTY
        ).blockingAwait();
        final Collection<String> keys = this.original.list(Key.ROOT)
            .join()
            .stream()
            .map(Key::string)
            .collect(Collectors.toList());
        MatcherAssert.assertThat(
            keys,
            new IsEqual<>(Arrays.asList("file2", "foo/file1"))
        );
    }

    @Test
    void movesItems() {
        final Key source = new Key.From("foo/file1");
        final Key destination = new Key.From("bla/file2");
        final byte[] bvalue = "my file1 content"
            .getBytes(StandardCharsets.UTF_8);
        this.original.save(
            source, new Content.From(bvalue)
        ).join();
        this.original.save(
            destination, Content.EMPTY
        ).join();
        this.wrapper.move(source, destination).blockingAwait();
        MatcherAssert.assertThat(
            new BlockingStorage(this.original)
                .value(destination),
            new IsEqual<>(bvalue)
        );
    }

    @Test
    @SuppressWarnings("deprecation")
    void readsSize() {
        final Key key = new Key.From("file.txt");
        final String text = "my file content";
        this.original.save(
            key,
            new Content.From(
                text.getBytes(StandardCharsets.UTF_8)
            )
        ).join();
        MatcherAssert.assertThat(
            this.wrapper.size(key).blockingGet(),
            new IsEqual<>((long) text.length())
        );
    }

    @Test
    void readsMeta() {
        final Key key = new Key.From("testmeta.txt");
        final String text = "my content for meta test";
        final Instant beforeSaveInstant = Instant.now().minusSeconds(2);
        this.original.save(
                key,
                new Content.From(
                        text.getBytes(StandardCharsets.UTF_8)
                )
        ).join();
        this.wrapper.value(key).blockingGet();
        Meta metadata = this.wrapper.metadata(key).blockingGet();
        metadata.read(Meta.OP_UPDATED_AT)
                .ifPresentOrElse(updatedAt ->
                                Assumptions.assumeTrue(updatedAt.isAfter(beforeSaveInstant)
                                        , "updated-at is incorrect")
                        , () -> fail("No updated-at metadata"));
        metadata.read(Meta.OP_ACCESSED_AT)
                .ifPresentOrElse(accessedAt ->
                                Assumptions.assumeTrue(accessedAt.isAfter(beforeSaveInstant)
                                        , "accessed-at is incorrect")
                        , () -> fail("No accessed-at metadata"));
    }

    @Test
    void readsValue() {
        final Key key = new Key.From("a/z");
        final byte[] bvalue = "value to read"
            .getBytes(StandardCharsets.UTF_8);
        this.original.save(
            key, new Content.From(bvalue)
        ).join();
        MatcherAssert.assertThat(
            new Remaining(
                new Concatenation(
                    this.wrapper.value(key).blockingGet()
                ).single()
                    .blockingGet(),
                true
            ).bytes(),
            new IsEqual<>(bvalue)
        );
    }

    @Test
    void deletesItem() throws Exception {
        final Key key = new Key.From("key_to_delete");
        this.original.save(key, Content.EMPTY).join();
        this.wrapper.delete(key).blockingAwait();
        MatcherAssert.assertThat(
            this.original.exists(key).get(),
            new IsEqual<>(false)
        );
    }

    @Test
    void runsExclusively() {
        final Key key = new Key.From("exclusively_key");
        final Function<RxStorage, Single<Integer>> operation = sto -> Single.just(1);
        this.wrapper.exclusively(key, operation).blockingGet();
        MatcherAssert.assertThat(
            this.wrapper.exclusively(key, operation).blockingGet(),
            new IsEqual<>(1)
        );
    }

    @Test
    void testSchedulingRxStorageWrapperS3() {
        final Key key = new Key.From("test.txt");
        final String data = "five\tsix eight";
        final Executor executor = Executors.newSingleThreadExecutor();
        final RxStorageWrapper rxsto = new RxStorageWrapper(this.original);
        rxsto.save(key, new Content.From(data.getBytes(StandardCharsets.US_ASCII))).blockingAwait();
        final String result = this.original.value(key).thenApplyAsync(content -> {
            MatcherAssert.assertThat("Values must match", content.asString().equals(data));
            return rxsto.value(key).to(ContentAs.STRING).to(SingleInterop.get()).thenApply(s -> s).toCompletableFuture().join();
        }, executor).toCompletableFuture().join();
        MatcherAssert.assertThat("Values must match", result.equals(data));
    }

    @Test
    void cleanup() throws ExecutionException, InterruptedException {
        this.original.save(new Key.From("aged_key1"), Content.EMPTY).join();
        this.original.save(new Key.From("aged_key2"), Content.EMPTY).join();
        TimeUnit.SECONDS.sleep(5);
        this.original.save(new Key.From("recent_key1"), Content.EMPTY).join();
        this.original.save(new Key.From("recent_key2"), Content.EMPTY).join();
        this.original.save(new Key.From("recent_key3"), Content.EMPTY).join();

        var cleanupPolicy = new CleanupPolicy();
        cleanupPolicy.setMaxAge(Duration.ofSeconds(4));
        var report = Cleaner.cleanup(this.original, cleanupPolicy).get();
        MatcherAssert.assertThat(
                report.getNbAged(),
                new IsEqual<>(2)
        );
        MatcherAssert.assertThat(
                report.getTotalCleaned(),
                new IsEqual<>(2)
        );
        MatcherAssert.assertThat("aged_key1 should have been cleaned up", !this.original.exists(new Key.From("aged_key1")).get());
        MatcherAssert.assertThat("aged_key1 should have been cleaned up", !this.original.exists(new Key.From("aged_key2")).get());
        MatcherAssert.assertThat("recent_key2 should not have been cleaned up", this.original.exists(new Key.From("recent_key2")).get());
        MatcherAssert.assertThat("recent_key3 should not have been cleaned up", this.original.exists(new Key.From("recent_key3")).get());

    }
}
