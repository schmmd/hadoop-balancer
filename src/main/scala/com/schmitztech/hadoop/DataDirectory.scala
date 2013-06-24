package com.schmitztech.hadoop

import java.io.File
import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils

case class DataDirectory(val file: File) {
  def freeSpaceOnDisk = file.getUsableSpace()
  def humanFreeSpaceOnDisk = 
    FileUtils.byteCountToDisplaySize(freeSpaceOnDisk)

  def size = blocks.iterator.map(_.size).sum
  def humanSize = FileUtils.byteCountToDisplaySize(size)

  private val metaFileRegex = "(blk_[+-]?\\d+)_\\d+.meta".r
  def blocks = {
    for {
      metaFile <- FileUtils.listFiles(file, Array("meta"), true).asScala
      metaFileRegex(blockFile) = metaFile.getName
    } yield {
      Block(new File(metaFile.getParent, blockFile), metaFile)
    }
  }
}
