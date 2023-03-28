package com.example.localstacktranscriberepro

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.netty.handler.logging.LogLevel
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import mu.KotlinLogging
import org.reactivestreams.Subscriber
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToFlux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toMono
import reactor.netty.http.client.HttpClient
import reactor.netty.resources.ConnectionProvider
import reactor.netty.transport.logging.AdvancedByteBufFormat
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.core.async.AsyncRequestBody
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.metrics.MetricCollection
import software.amazon.awssdk.metrics.MetricPublisher
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.S3Configuration
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
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
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus.COMPLETED
import java.net.URI
import java.nio.ByteBuffer
import java.time.Duration
import java.util.Optional

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

    private val webclient =
        webClient(
            webClientBuilder = webClientBuilder,
            verboseLogging = false,
            connectionProviderName = "mp3-retriever",
            pendingMaxCount = PENDING_ACQUISITION_MAX_COUNT,
            pendingAcquireTimeout = PENDING_ACQUIRE_TIMEOUT,
            idleTimeoutSeconds = 10
        )

    private val transcribeClient: TranscribeAsyncClient =
        TranscribeAsyncClient.builder().httpClient(
            NettyNioAsyncHttpClient.builder()
                .readTimeout(Duration.ofSeconds(READ_TIMEOUT))
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
            .readTimeout(Duration.ofSeconds(READ_TIMEOUT))
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
    ): Mono<String> {
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
            .map { it.transcriptionJob().transcriptionJobStatus().name }
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

    fun storeFileOnS3(fileKey: String, fileUrl: String): Mono<Void> {
        return webclient
            .get()
            .uri(fileUrl)
            .exchangeToMono {
                s3Client.putObject(
                    PutObjectRequest.builder()
                        .key(fileKey)
                        .bucket(s3bucket).build(),
                    object : AsyncRequestBody {
                        override fun subscribe(s: Subscriber<in ByteBuffer>) {
                            it.bodyToFlux<ByteBuffer>().subscribe(s)
                        }

                        override fun contentLength(): Optional<Long> {
                            val contentLength = it.headers().contentLength()
                            return Optional.ofNullable<Long>(
                                if (contentLength.isPresent) {
                                    contentLength.asLong
                                } else {
                                    null
                                }
                            )
                        }
                    }
                ).toMono().then()
            }
    }

    fun removeFileFromS3(fileKey: String): Mono<Void> =
        s3Client.deleteObject(
            DeleteObjectRequest.builder().bucket(s3bucket).key(fileKey).build()
        ).toMono().then()

    @Suppress("LongParameterList")
    private fun webClient(
        webClientBuilder: WebClient.Builder,
        verboseLogging: Boolean,
        baseUrl: String? = null,
        connectionProviderName: String,
        pendingMaxCount: Int,
        connectTimeout: Long = 5,
        readTimeout: Long = 5,
        writeTimeout: Int = 5,
        pendingAcquireTimeout: Long = ConnectionProvider.DEFAULT_POOL_ACQUIRE_TIMEOUT,
        idleTimeoutSeconds: Long = AWS_TIMEOUT
    ): WebClient = webClientBuilder.build().mutate().clientConnector(
        ReactorClientHttpConnector(
            HttpClient.create(
                ConnectionProvider.builder(connectionProviderName)
                    .maxIdleTime(Duration.ofSeconds(idleTimeoutSeconds))
                    .maxConnections(ConnectionProvider.DEFAULT_POOL_MAX_CONNECTIONS)
                    .pendingAcquireMaxCount(pendingMaxCount)
                    .pendingAcquireTimeout(Duration.ofMillis(pendingAcquireTimeout))
                    .metrics(true)
                    .build()
            ).followRedirect(true).apply {
                if (verboseLogging) {
                    wiretap(
                        "reactor.netty.http.client.HttpClient",
                        LogLevel.DEBUG,
                        AdvancedByteBufFormat.TEXTUAL
                    )
                }
            }.responseTimeout(Duration.ofSeconds(connectTimeout))
                .doOnConnected { connection ->
                    connection.addHandlerLast(ReadTimeoutHandler(readTimeout.toInt()))
                        .addHandlerLast(WriteTimeoutHandler(writeTimeout))
                }
        )
    )
        .apply {
            if (baseUrl != null) {
                it.baseUrl(baseUrl)
            }
        }
        .build()

    companion object {
        const val AWS_TIMEOUT = 340L
        const val READ_TIMEOUT = 5L
        const val MAX_CONNECTIONS = 256
        const val PENDING_ACQUISITION_MAX_COUNT = -1
        const val PENDING_ACQUIRE_TIMEOUT = 180000L
    }
}
