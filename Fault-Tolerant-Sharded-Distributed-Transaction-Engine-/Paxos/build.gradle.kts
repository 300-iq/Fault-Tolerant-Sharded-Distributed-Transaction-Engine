// ✅ Needed so the Kotlin DSL can see protobuf{} / protoc{} / generateProtoTasks{}

import com.google.protobuf.gradle.*



plugins {
    id("java")
    id("com.google.protobuf") version "0.9.4"
}



group = "paxos"

version = "1.0-SNAPSHOT"



repositories {

    mavenCentral()

    // google() // optional; mavenCentral is enough for these artifacts

}

val grpcVersion = "1.64.0"
val protobufVersion = "3.25.3"

dependencies {
    implementation("io.grpc:grpc-netty-shaded:$grpcVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("com.google.protobuf:protobuf-java:$protobufVersion")

    // CLI & CSV utilities
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.2")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.17.2")

    // Concurrency helpers
    implementation("com.google.guava:guava:33.3.0-jre")
    implementation("org.mapdb:mapdb:3.0.8") // Added MapDB as a dependency

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testImplementation("io.grpc:grpc-inprocess:$grpcVersion")
    testImplementation("io.grpc:grpc-testing:$grpcVersion")

    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
}

java {
    toolchain {

        languageVersion.set(JavaLanguageVersion.of(21))

    }

}



tasks.test {

    useJUnitPlatform()

}



// --- Protobuf / gRPC codegen ---

protobuf {

    protoc {

        artifact = "com.google.protobuf:protoc:$protobufVersion"

    }

    plugins {

        id("grpc") {

            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"

        }

    }

    generateProtoTasks {

        // Apply the grpc plugin to all proto generation tasks

        all().forEach { task ->

            task.plugins {

                id("grpc")

            }

        }

    }

}



// Make generated sources visible to IDE/Java compilation

sourceSets {

    val main by getting {

        java.srcDirs(

            "build/generated/source/proto/main/java",

            "build/generated/source/proto/main/grpc"

        )

    }

}

// ---- CLI helpers ----
// Print the runtime classpath (used by scripts to run with `java -cp ...`)
tasks.register("printRuntimeClasspath") {
    group = "application"
    description = "Prints the runtime classpath for the main source set"
    doLast {
        val cp = sourceSets["main"].runtimeClasspath
        println(cp.asPath)
    }
}

// Run a single Paxos node from Gradle with -D or -P properties forwarded.
// Example:
//   gradlew runNode -DnodeId=1 -Dport=50051 -Dleader=1 -Dpeer.1=localhost:50051 -Dpeer.2=localhost:50052
tasks.register<JavaExec>("runNode") {
    group = "application"
    description = "Run a Paxos node (NodeServer)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("paxos.node.NodeServer")
    standardInput = System.`in`

    // Forward selected -D system properties from Gradle JVM to the launched process
    val allowed = setOf(
        // identity / topology
        "nodeId", "port", "leader",
        // timing knobs
        "heartbeat", "minLeaderTimeout", "maxLeaderTimeout",
        "minPrepareBackoff", "maxPrepareBackoff", "replicationTimeout", "prepareThrottle",
        "replTimeoutMs", "followerForwardTimeoutMs",
        // persistence / checkpointing / logging
        "enableCheckpointing", "checkpointPeriod", "auditMaxEntries",
        "dataDir", "dbPath", "resetDb",
        "debugLogs"
    )
    val forward = mutableMapOf<String, Any>()
    System.getProperties().forEach { k, v ->
        val key = k.toString()
        if (key.startsWith("peer.")) {
            forward[key] = v
        } else if (allowed.contains(key)) {
            forward[key] = v
        }
    }
    // Also allow passing via -P properties (e.g., -PnodeId=1)
    project.properties.forEach { (k, v) ->
        if (k.startsWith("peer.") || allowed.contains(k)) {
            forward.putIfAbsent(k, v as Any)
        }
    }
    systemProperties(forward)
    jvmArgs("-Dfile.encoding=UTF-8")
}

// Run the client with a CSV file. Pass CSV path using --args or -Pcsv.
// Examples:
//   gradlew runClient --args "src/test/csv tests/New Test Cases.csv" -DleaderTarget=localhost:50051 -Dpeer.1=localhost:50051 ...
//   gradlew runClient -Pcsv="src/test/csv tests/New Test Cases.csv" -PleaderTarget=localhost:50051 -Ppeer.1=localhost:50051 ...
tasks.register<JavaExec>("runClient") {
    group = "application"
    description = "Run the Paxos client (ClientApp)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("paxos.client.ClientApp")
    standardInput = System.`in`

    // Determine CSV argument
    val csvFromArgs = (project.findProperty("args") as String?)
    val csvFromProp = (project.findProperty("csv") as String?)
    val csv = csvFromArgs ?: csvFromProp
    if (csv != null && csv.isNotBlank()) {
        args(csv)
    } else {
        // If not provided at configuration time, we still allow passing via --args at execution,
        // so do not fail here.
    }

    val allowed = setOf(
        // client-side tunables
        "leaderTarget", "bootstrapTarget",
        "clientTimeoutMs", "clientRetryBackoffMs", "clientMaxAttempts", "quiescenceTimeoutMs",
        // display behavior
        "persistDBOnDisk",
        // logging
        "debugLogs"
    )
    val forward = mutableMapOf<String, Any>()
    System.getProperties().forEach { k, v ->
        val key = k.toString()
        if (key.startsWith("peer.") || allowed.contains(key)) {
            forward[key] = v
        }
    }
    project.properties.forEach { (k, v) ->
        if (k.startsWith("peer.") || allowed.contains(k)) {
            forward.putIfAbsent(k, v as Any)
        }
    }
    systemProperties(forward)
    jvmArgs("-Dfile.encoding=UTF-8")
}

