import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.values.TypeDescriptors

object Main {

  trait Options extends StreamingOptions {
    @Description("Input text to print.")
    @Default.String("My input text")
    def getInputText(): String
    def setInputText(value: String): Unit
  }

  def main(args: Array[String]): Unit = {
    val options: Options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[Options])

    val pipeline: Pipeline = Pipeline.create(options)
    pipeline
      .apply(
        "Create elements",
        Create.of("Hello", "World!", options.getInputText)
      )
      .apply(
        "Print elements",
        MapElements
          .into(TypeDescriptors.strings())
          .via((x: String) => {
            println(x)
            x
          })
      )
    pipeline.run()
  }

}
