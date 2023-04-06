package com.example.localstacktranscriberepro

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import mu.KotlinLogging
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.internal.async.FileAsyncRequestBody
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.metrics.MetricCollection
import software.amazon.awssdk.metrics.MetricPublisher
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectResponse
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.transcribe.TranscribeAsyncClient
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest
import software.amazon.awssdk.services.transcribe.model.JobExecutionSettings
import software.amazon.awssdk.services.transcribe.model.LanguageCode
import software.amazon.awssdk.services.transcribe.model.Media
import software.amazon.awssdk.services.transcribe.model.MediaFormat
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobRequest
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobResponse
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus.COMPLETED
import java.net.URI
import java.nio.file.Paths
import java.time.Duration

@Suppress("LongParameterList")
class AmazonTranscriptionRequester(
    webClientBuilder: WebClient.Builder,
    region: String,
    credentialsProvider: AwsCredentialsProvider,
    s3endpointOverride: URI? = null,
    transcriptionEndpointOverride: URI? = null,
    private val s3bucket: String,
    private val objectMapper: ObjectMapper
) {
    private val logger = KotlinLogging.logger("pp.transcription.amazon")

    private val transcribeClient: TranscribeAsyncClient =
        TranscribeAsyncClient.builder().httpClient(
            NettyNioAsyncHttpClient.builder()
                .writeTimeout(Duration.ofSeconds(WRITE_TIMEOUT))
                .readTimeout(Duration.ofSeconds(READ_TIMEOUT))
                .connectionMaxIdleTime(Duration.ofSeconds(IDLE_TIMEOUT))
                .maxConcurrency(MAX_CONNECTIONS)
                .build()
        )
            .region(Region.of(region))
            .apply {
                if (transcriptionEndpointOverride != null) {
                    endpointOverride(transcriptionEndpointOverride)
                }
            }
            .credentialsProvider(credentialsProvider)
            .overrideConfiguration { o ->
                o.addMetricPublisher(object : MetricPublisher {
                    override fun close() {
                        // nop
                    }

                    override fun publish(metricCollection: MetricCollection) {
                        logger.debug { "children: " + metricCollection.children() }
                        logger.debug { "metrics: " + metricCollection.iterator().asSequence().toList() }
                    }
                })
            }
            .build()

    val s3Client: S3AsyncClient = S3AsyncClient.builder().httpClient(
        NettyNioAsyncHttpClient.builder()
            .writeTimeout(Duration.ofSeconds(WRITE_TIMEOUT))
            .readTimeout(Duration.ofSeconds(READ_TIMEOUT))
            .connectionMaxIdleTime(Duration.ofSeconds(IDLE_TIMEOUT))
            .maxConcurrency(MAX_CONNECTIONS)
            .build()
    )
        .region(Region.of(region))
        .apply {
            if (s3endpointOverride != null) {
                endpointOverride(s3endpointOverride)
            }
        }
        .credentialsProvider(credentialsProvider)
        .overrideConfiguration { o ->
            o.addMetricPublisher(object : MetricPublisher {
                override fun close() {
                    // nop
                }

                override fun publish(metricCollection: MetricCollection) {
                    logger.debug { "children: " + metricCollection.children() }
                    logger.debug { "metrics: " + metricCollection.iterator().asSequence().toList() }
                }
            })
        }
        .serviceConfiguration(
            S3Configuration.builder()
                .checksumValidationEnabled(true)
                .chunkedEncodingEnabled(true)
                .build()
        ).build()

    fun startTranscription(
        jobName: String,
        s3Location: String,
        outputKey: String
    ): Mono<StartTranscriptionJobResponse> {
        return transcribeClient.startTranscriptionJob(
            StartTranscriptionJobRequest.builder()
                .transcriptionJobName(jobName)
                .outputBucketName(s3bucket)
                .outputKey(outputKey)
                .languageCode(LanguageCode.EN_US)
                .mediaFormat(MediaFormat.MP3)
                .jobExecutionSettings(JobExecutionSettings.builder().allowDeferredExecution(true).build())
                .media(Media.builder().mediaFileUri(s3Location).build())
                .build()
        ).toMono()
    }

    fun getTranscriptIfCompleted(jobName: String, transcriptionOutputKey: String): Mono<String> {
        return transcribeClient.getTranscriptionJob(
            GetTranscriptionJobRequest.builder().transcriptionJobName(jobName).build()
        ).toMono()
            .flatMap { transcriptionJobResponse ->
                if (transcriptionJobResponse.transcriptionJob().transcriptionJobStatus() == COMPLETED) {
                    s3Client.getObject(
                        GetObjectRequest.builder().bucket(s3bucket).key(transcriptionOutputKey).build(),
                        AsyncResponseTransformer.toBytes()
                    ).toMono()
                        .map(ResponseBytes<GetObjectResponse>::asUtf8String)
                        .map {
                            objectMapper.readValue<TranscriptionResultContainer>(it)
                                .results
                                .transcripts
                                .first()
                                .transcript
                        }
                } else {
                    Mono.empty()
                }
            }
    }

    private data class TranscriptionResultContainer(val results: TranscriptionResult)
    private data class TranscriptionResult(val transcripts: List<Transcript>)
    private data class Transcript(val transcript: String)

    fun storeFileOnS3(fileKey: String): Mono<Void> {
        return s3Client.putObject(
            PutObjectRequest.builder()
                .key(fileKey)
                .bucket(s3bucket).build(),
            FileAsyncRequestBody.builder().path(Paths.get(
                "src",
                "test",
                "resources",
                "sfx-words-yes.mp3"
            ))
                .build()
        ).toMono().then()
    }

    companion object {
        const val AWS_TIMEOUT = 340L
        const val READ_TIMEOUT = 240L
        const val WRITE_TIMEOUT = 240L
        const val IDLE_TIMEOUT = 240L
        const val MAX_CONNECTIONS = 256
        const val PENDING_ACQUISITION_MAX_COUNT = -1
        const val PENDING_ACQUIRE_TIMEOUT = 180000L
    }
}
