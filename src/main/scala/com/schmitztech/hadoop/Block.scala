package com.schmitztech.hadoop

import java.io.File
import org.apache.commons.io.FileUtils

case class Block(blockFile: File, metaFile: File, size: Long) {
  require(blockFile.exists())
  require(metaFile.exists())

  def humanSize = FileUtils.byteCountToDisplaySize(size)
}

object Block {
  def apply(blockFile: File, metaFile: File): Block = {
    new Block(blockFile, metaFile, FileUtils.sizeOf(blockFile) + FileUtils.sizeOf(metaFile))
  }
}
