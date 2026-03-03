Spark batch

Requires JDK 17

# Replace Java installation guide with the below:
# check current java
java -version

# install OpenJDK 17
sudo apt update
sudo apt install -y openjdk-17-jdk

# set JAVA_HOME for current shell (adjust path if different)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH="$JAVA_HOME/bin:$PATH"

# persist for future shells
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc

# verify
java -version