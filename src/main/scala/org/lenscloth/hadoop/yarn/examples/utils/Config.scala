package org.lenscloth.hadoop.yarn.examples.utils

case class Config(name: String ="",
                  stagingDir: String = "",
                  command: String = "",
                  resources: Seq[String] = Seq.empty[String],
                  envs: Map[String, String] = Map.empty[String, String])

object CLI {
  private val parser = new scopt.OptionParser[Config]("spread") {
    head("spread", "1.0")

    opt[String]('n', "name").action((x,c) => c.copy(name = x)).text("Name of job").required()
    opt[String]('s', "staging_dir").action((x,c) => c.copy(stagingDir = x)).text("Staging directory in HDFS").required()
    opt[String]('c',"am_command").action( (x, c) => c.copy(command = x)).text("Command for application master").required()

    opt[Seq[String]]('r',"resources").action( (x, c) => c.copy(resources =  x)).text("Resources that will be loaded on each container")
    opt[Map[String, String]]('e',"environment_variable").valueName("k1=v1, k2=v2 , ...").action((x,c) => c.copy(envs = x)).text("Environment variable for each container")
  }

  def parse(args: List[String]): Option[Config] = parser.parse(args, Config())
}