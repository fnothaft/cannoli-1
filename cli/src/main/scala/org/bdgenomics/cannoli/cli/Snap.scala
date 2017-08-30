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
package org.bdgenomics.cannoli.cli

import htsjdk.samtools.ValidationStringency
import org.apache.spark.SparkContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.fragment.{ FragmentRDD, InterleavedFASTQInFormatter }
import org.bdgenomics.adam.rdd.read.{ AlignmentRecordRDD, AnySAMOutFormatter }
import org.bdgenomics.formats.avro.AlignmentRecord
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object Snap extends BDGCommandCompanion {
  val commandName = "snap"
  val commandDescription = "ADAM Pipe API wrapper for Snap."

  def apply(cmdLine: Array[String]) = {
    new Snap(Args4j[SnapArgs](cmdLine))
  }
}

class SnapArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "Location to pipe from, in interleaved FASTQ format.", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to pipe to.", index = 1)
  var outputPath: String = null

  @Args4jOption(required = true, name = "-index", usage = "Path of the index directory.")
  var indexPath: String = null

  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file.")
  var asSingleFile: Boolean = false

  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output.")
  var deferMerging: Boolean = false

  @Args4jOption(required = false, name = "-disable_fast_concat", usage = "Disables the parallel file concatenation engine.")
  var disableFastConcat: Boolean = false

  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to STRICT.")
  var stringency: String = "STRICT"

  @Args4jOption(required = false, name = "-add_indices", usage = "Adds index files via SparkFiles mechanism.")
  var addIndices: Boolean = false

  @Args4jOption(required = false, name = "-docker_image", usage = "Docker image to use. Defaults to quay.io/ucsc_cgl/snap:1.0beta.18--07475052cd34f17055991cec339898ff0e8bd07d")
  var dockerImage: String = "quay.io/ucsc_cgl/snap:1.0beta.18--07475052cd34f17055991cec339898ff0e8bd07d"

  @Args4jOption(required = false, name = "-use_docker", usage = "If true, uses Docker to launch BWA. If false, uses the BWA executable path.")
  var useDocker: Boolean = false

  @Args4jOption(required = false, name = "-docker_cmd", usage = "The docker command to run. Defaults to 'docker'.")
  var dockerCmd: String = "docker"

  // must be defined due to ADAMSaveAnyArgs, but unused here
  var sortFastqOutput: Boolean = false

  @Args4jOption(required = false, name = "-snap_path", usage = "Path to the snap-aligner executable. Defaults to snap-aligner.")
  var snapPath: String = "snap-aligner"
}

/**
 * Snap.
 */
class Snap(protected val args: SnapArgs) extends BDGSparkCommand[SnapArgs] with Logging {
  val companion = Snap
  val stringency: ValidationStringency = ValidationStringency.valueOf(args.stringency)

  def run(sc: SparkContext) {
    // SNAP validates the read suffixes when reading interleaved FASTQ
    sc.hadoopConfiguration.setBoolean(FragmentRDD.WRITE_SUFFIXES, true)

    val input: FragmentRDD = sc.loadFragments(args.inputPath)

    implicit val tFormatter = InterleavedFASTQInFormatter
    implicit val uFormatter = new AnySAMOutFormatter

    val command = Seq(args.snapPath,
      "paired",
      args.indexPath,
      "-t", "1",
      "-pairedInterleavedFastq", "-",
      "-o", "-bam", "-",
      "-map", "-pre")
    val output: AlignmentRecordRDD = input.pipe[AlignmentRecord, AlignmentRecordRDD, InterleavedFASTQInFormatter](command.mkString(" "))

    output.save(args)
  }
}
