hadoop fs -mkdir colbySnedekerProject3/input
hadoop fs -put sample.txt colbySnedekerProject3/input

mkdir mutualFriendFinder_class
javac -classpath /opt/hadoop/hadoop-core-1.2.1.jar -d mutualFriendFinder_class MutualFriendFinder.java
jar -cvf MutualFriendFinder.jar -C /home/vcslstudent/colbySnedekerProject3/mutualFriendFinder_class/ .
hadoop jar /home/vcslstudent/colbySnedekerProject3/MutualFriendFinder.jar snedeker.cc.project3.MutualFriendFinder /user/vcslstudent/colbySnedekerProject3/input/sample.txt /user/vcslstudent/colbySnedekerProject3/MutualFriendFinder_out_java

hadoop fs -cat /user/vcslstudent/colbySnedekerProject3/MutualFriendFinder_out_java/part-r-00000