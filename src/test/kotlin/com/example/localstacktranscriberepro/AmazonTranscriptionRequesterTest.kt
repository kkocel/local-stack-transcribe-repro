package com.example.localstacktranscriberepro

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldNotStartWith
import org.junit.jupiter.api.Test
import org.springframework.web.reactive.function.client.WebClient
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.localstack.LocalStackContainer.EnabledService
import org.testcontainers.containers.localstack.LocalStackContainer.Service
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import reactor.kotlin.core.publisher.toMono
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.services.s3.model.CreateBucketRequest

@Testcontainers
class AmazonTranscriptionRequesterTest {

    @Container
    val localstack: LocalStackContainer =
        LocalStackContainer(DockerImageName.parse("localstack/localstack:latest-amd64"))
            .withServices(
                Service.S3,
                EnabledService.named("transcribe")
            )

    @Test
    fun `get file from internet and save it to s3`() {
        val s3bucket = "kkocel-localstack-repro"
        val requester = AmazonTranscriptionRequester(
            webClientBuilder = WebClient.builder(),
            region = localstack.region,
            credentialsProvider = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    localstack.accessKey,
                    localstack.secretKey
                )
            ),
            s3endpointOverride = localstack.getEndpointOverride { "s3" },
            transcriptionEndpointOverride = localstack.getEndpointOverride { "transcript" },
            s3bucket = s3bucket,
            objectMapper = ObjectMapper().findAndRegisterModules()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        )

        requester.s3Client.createBucket(CreateBucketRequest.builder().bucket(s3bucket).build()).toMono().block()

        requester.storeFileOnS3(
            "fileKey",
            "https://dcs.megaphone.fm/CAD6920676170.mp3?key=12892c23e53213cdbd361e1a37757cca&request_event_id=" +
                "c928e9e3-201f-412e-b424-61ad17cbeadb&via=012c48d4-27b3-11ed-a82e-df744c4c2b8f"
        )
            .block()

        val name = "job-name-2"
        val trscptRsp =
            requester.startTranscription(
                jobName = name,
                s3Location = "s3://$s3bucket/fileKey",
                outputKey = "output-transcription"
            ).block()

        trscptRsp?.transcriptionJob() shouldNotBe null

        requester.getTranscriptIfCompleted(jobName = name, transcriptionOutputKey = "output-transcription")
            .block() shouldNotStartWith "https://"
    }
}
