import org.gradle.plugins.signing.SigningExtension

plugins {
  id("com.android.lint")
  id("com.eygraber.conventions-kotlin-multiplatform")
  id("com.eygraber.conventions-android-kmp-library")
  id("com.eygraber.conventions-detekt2")
  id("com.eygraber.conventions-publish-maven-central")
}

val requestedTaskNames = gradle.startParameter.taskNames
val isMavenLocalOnlyPublication =
  requestedTaskNames.any { it.endsWith("ToMavenLocal") } &&
    requestedTaskNames.none { taskName ->
      taskName == "publish" ||
        taskName.endsWith(":publish") ||
        taskName.contains("MavenCentral") ||
        taskName.contains("Repository")
    }

plugins.withId("signing") {
  extensions.configure<SigningExtension> {
    setRequired(!isMavenLocalOnlyPublication)
  }
}

kotlin {
  defaultKmpTargets(
    project = project,
    androidNamespace = "com.eygraber.sqldelight.androidx.driver",
  )

  android {
    withHostTest {}

    withDeviceTest {
      instrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"

      managedDevices {
        localDevices {
          create("pixel2Api35") {
            device = "Pixel 2"
            apiLevel = 35
            testedAbi = "x86_64"
            systemImageSource = "aosp-atd"
          }
        }
      }
    }
  }

  sourceSets {
    named("androidDeviceTest") {
      kotlin.srcDir("src/androidInstrumentedTest/kotlin")

      dependencies {
        implementation(libs.androidx.sqliteFramework)

        implementation(libs.test.junit)
        implementation(libs.test.androidx.core)
        implementation(libs.test.androidx.runner)

        implementation(libs.test.kotlin)
        implementation(libs.test.kotlinx.coroutines)
      }
    }

    named("androidHostTest") {
      kotlin.srcDir("src/androidUnitTest/kotlin")

      dependencies {
        implementation(libs.androidx.sqliteFramework)

        implementation(libs.test.junit)
        implementation(libs.test.androidx.core)
        implementation(libs.test.robolectric)
      }
    }

    commonMain.dependencies {
      implementation(libs.androidx.collections)

      api(libs.androidx.sqlite)
      api(libs.cashapp.sqldelight.runtime)

      implementation(libs.atomicfu)
      implementation(libs.kotlinx.coroutines.core)
    }

    commonTest.dependencies {
      implementation(libs.kotlinx.coroutines.core)

      implementation(libs.test.kotlin)
      implementation(libs.test.kotlinx.coroutines)
    }

    jvmTest.dependencies {
      implementation(libs.androidx.sqliteBundled)
      implementation(libs.test.kotlin.junit)
    }

    nativeTest.dependencies {
      implementation(libs.androidx.sqliteBundled)
      implementation(libs.okio)
    }
  }
}
