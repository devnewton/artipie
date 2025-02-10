/*
 * The MIT License (MIT) Copyright (c) 2020-2023 artipie.com
 * https://github.com/artipie/artipie/blob/master/LICENSE.txt
 */
package com.artipie.asto.s3;

import com.artipie.asto.Meta;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.s3.model.GetObjectTaggingResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Metadata from S3 object.
 * @since 0.1
 */
final class S3HeadMeta implements Meta {

    /**
     * S3 head object response.
     */
    private final HeadObjectResponse headObjectResponse;

    /**
     * S3 tagging object response.
     */
    private final GetObjectTaggingResponse taggingObjectResponse;

    /**
     * New metadata.
     *
     * @param headObjectResponse  Head response
     * @param taggingObjectResponse  Tagging response
     */
    S3HeadMeta(final HeadObjectResponse headObjectResponse, GetObjectTaggingResponse taggingObjectResponse) {
        this.headObjectResponse = headObjectResponse;
        this.taggingObjectResponse = taggingObjectResponse;
    }

    @Override
    public <T> T read(final ReadOperator<T> opr) {
        final Map<String, String> raw = new HashMap<>();
        Meta.OP_SIZE.put(raw, this.headObjectResponse.contentLength());
        // ETag is a quoted MD5 of blob content according to S3 docs
        Meta.OP_MD5.put(raw, this.headObjectResponse.eTag().replaceAll("\"", ""));
        if(null != this.headObjectResponse.lastModified()) {
            Meta.OP_UPDATED_AT.put(raw, this.headObjectResponse.lastModified());
        }
        if(null != this.taggingObjectResponse && this.taggingObjectResponse.hasTagSet()) {
            for(var tag : this.taggingObjectResponse.tagSet()) {
                if("Last-Accessed".equals(tag.key())) {
                    Meta.OP_ACCESSED_AT.put(raw, Instant.from(RFC_1123_DATE_TIME.parse(tag.value())));
                }
            }
        }
        return opr.take(raw);
    }
}
