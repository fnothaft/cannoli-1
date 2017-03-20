/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.cannoli

import htsjdk.samtools.ValidationStringency
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.fragment.{ FragmentRDD, InterleavedFASTQInFormatter }
import org.bdgenomics.adam.rdd.read.{ AlignmentRecordRDD, AnySAMOutFormatter }
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Bwa extends BDGCommandCompanion {
  val commandName = "bwa"
  val commandDescription = "ADAM Pipe API wrapper for Bwa."

  def apply(cmdLine: Array[String]) = {
    new Bwa(Args4j[BwaArgs](cmdLine))
  }
}

class BwaArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "Location to pipe from, in interleaved FASTQ format", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to pipe to", index = 1)
  var outputPath: String = null

  @Argument(required = true, metaVar = "SAMPLE", usage = "Sample ID", index = 2)
  var sample: String = null

  @Args4jOption(required = true, name = "-index", usage = "Path to the bwa index to be searched, e.g. <ebwt> in bwa [options]* <ebwt> ...")
  var indexPath: String = null

  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false

  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output")
  var deferMerging: Boolean = false

  @Args4jOption(required = false, name = "-bwa_path", usage = "Path to the BWA executable.")
  var bwaPath: String = _

  @Args4jOption(required = false, name = "-docker_image", usage = "Docker image to use. Defaults to quay.io/ucsc_cgl/bwa:0.7.12--256539928ea162949d8a65ca5c79a72ef557ce7c..")
  var dockerImage: String = "quay.io/ucsc_cgl/bwa:0.7.12--256539928ea162949d8a65ca5c79a72ef557ce7c"

  @Args4jOption(required = false, name = "-use_docker", usage = "If true, uses Docker to launch BWA. If false, uses the BWA executable path.")
  var useDocker: Boolean = _

  // must be defined due to ADAMSaveAnyArgs, but unused here
  var sortFastqOutput: Boolean = false
}

/**
 * Bwa.
 */
class Bwa(protected val args: BwaArgs) extends BDGSparkCommand[BwaArgs] with Logging {
  val companion = Bwa

  def run(sc: SparkContext) {
    val input: FragmentRDD = sc.loadFragments(args.inputPath)

    implicit val tFormatter = InterleavedFASTQInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val sample = args.sample

    val bwaCommand = if (args.useDocker) {
      Seq("docker",
        "run",
        args.dockerImage,
        "mem",
        "-t", "1",
        "-R", s"@RG\\tID:${sample}\\tLB:${sample}\\tPL:ILLUMINA\\tPU:0\\tSM:${sample}",
        "-p",
        args.indexPath,
        "-").mkString(" ")
    } else {
      require(args.bwaPath != null,
        "-bwaPath must be defined if not using Docker.")
      Seq(args.bwaPath,
        "mem",
        "-t", "1",
        "-R", s"@RG\tID:${sample}\tLB:${sample}\tPL:ILLUMINA\tPU:0\tSM:${sample}",
        "-p",
        args.indexPath,
        "-").mkString(" ")
    }

    val output: AlignmentRecordRDD = input.pipe[AlignmentRecord, AlignmentRecordRDD, InterleavedFASTQInFormatter](bwaCommand)

    output.save(args)
  }
}