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
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.ADAMSaveAnyArgs
import org.bdgenomics.adam.rdd.variant.{
  VariantContextRDD,
  VCFInFormatter,
  VCFOutFormatter
}
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object SnpEff extends BDGCommandCompanion {
  val commandName = "snpEff"
  val commandDescription = "ADAM Pipe API wrapper for SnpEff."

  def apply(cmdLine: Array[String]) = {
    new SnpEff(Args4j[SnpEffArgs](cmdLine))
  }
}

class SnpEffArgs extends Args4jBase with ADAMSaveAnyArgs with ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "Location to pipe from", index = 0)
  var inputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "Location to pipe to", index = 1)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-single", usage = "Saves OUTPUT as single file")
  var asSingleFile: Boolean = false

  @Args4jOption(required = false, name = "-defer_merging", usage = "Defers merging single file output")
  var deferMerging: Boolean = false

  @Args4jOption(required = false, name = "-stringency", usage = "Stringency level for various checks; can be SILENT, LENIENT, or STRICT. Defaults to STRICT")
  var stringency: String = "STRICT"

  // must be defined due to ADAMSaveAnyArgs, but unused here
  var sortFastqOutput: Boolean = false
}

/**
 * SnpEff.
 */
class SnpEff(protected val args: SnpEffArgs) extends BDGSparkCommand[SnpEffArgs] with Logging {
  val companion = SnpEff
  val stringency = ValidationStringency.valueOf(args.stringency)

  def run(sc: SparkContext) {
    val input: VariantContextRDD = sc.loadVcf(args.inputPath, stringency)

    implicit val tFormatter = VCFInFormatter
    implicit val uFormatter = new VCFOutFormatter(input.headerLines)

    val snpEffCommand = "snpEff -download GRCh38.82"
    val output: VariantContextRDD = input.pipe[VariantContext, VariantContextRDD, VCFInFormatter](snpEffCommand)
      .transform(_.cache())

    output.saveAsVcf(args.outputPath, args.asSingleFile, stringency)
  }
}
