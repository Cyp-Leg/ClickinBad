if [ "$#" -ne 2 ]; then
  echo "You should define both paths"
  exit 1
fi

sbt package
spark-submit --class "Main" --master local[2] target/scala-2.11/clickinbad_2.11-1.0.jar $1 $2

