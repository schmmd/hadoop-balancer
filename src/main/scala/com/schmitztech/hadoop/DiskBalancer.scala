package com.schmitztech.hadoop

import java.io.File

class DiskBalancer(directories: Seq[DataDirectory]) {
  val rand = new java.util.Random

  def status: String = 
    directories.map { dir =>
      s"${dir.file.getPath}:\n" +
      s"\t${dir.blocks.size} blocks\n" +
      s"\tBlocks: ${dir.humanSize}\n" +
      s"\tFree s: ${dir.humanFreeSpaceOnDisk}\n"
    }.mkString("\n")

  // variance in GB
  def variance(): Double = {
    val sizes = directories.map(_.freeSpaceOnDisk / 1024 / 1024 / 1024)
    val mean = sizes.sum.toDouble / sizes.size.toDouble

    sizes.map { size => 
      val diff = size - mean
      diff * diff
    }.sum.toDouble / sizes.size.toDouble
  }

  def step(): Boolean = {
    val initialVariance = variance()

    val emptiest = directories.maxBy(_.freeSpaceOnDisk)
    val fullest = directories.minBy(_.freeSpaceOnDisk)

    val blocks = fullest.blocks.toIndexedSeq
    val randomBlockIndex = rand.nextInt(blocks.size)
    val randomBlock = blocks(randomBlockIndex)

    move(randomBlock, emptiest)

    val finalVariance = variance()

    finalVariance < initialVariance
  }

  def run() {
    while (step()) {
    }
  }

  def move(block: Block, directory: DataDirectory) {
    val dest = {
      val path = block.blockFile.getParentFile.getPath
      val suffix = path.drop(path.indexOf("current"))
      val prefix = directory.file.getPath.take(path.indexOf("current"))
      prefix + "/" + suffix
    }

    println(s"Move '${block.blockFile.getPath}' to '$dest'...")
    // FileUtils.moveFileToDirectory(block.blockFile, dest)
    // FileUtils.moveFileToDirectory(block.metaFile, dest)
  }
}

object DiskBalancerMain extends App {
  val disks = args

  val balancer = new DiskBalancer(disks.map(path => DataDirectory(new File(path))))
  println(balancer.status)
  println(balancer.variance)

  balancer.step()
}
