package table_opt

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object basic_opt {
	def main (args: Array[String]): Unit = {
		val fsSettings = EnvironmentSettings
				.newInstance()
				.useBlinkPlanner()
				.inStreamingMode()
				.build()
		val fsTableEnv = TableEnvironment.create(fsSettings)


	}
}
