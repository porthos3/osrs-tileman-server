# osrs-tileman-server

Server for Conor Leckey's tileman mode Runelite plugin for Old School Runescape as found here:

https://github.com/ConorLeckey/Tileman-Mode

This server provides an API which the tileman mode plugin can interact with to provide multiplayer functionality to the tileman gamemode.

## Usage

1. Download and unpack the latest released zip archive.
2. Edit properties.json to specify your desired secret token and which port the server should run on
3. [Port-forward](https://www.howtogeek.com/66214/how-to-forward-ports-on-your-router/) that port through any routers/modems you have
4. Ensure you have a JRE (Java Runtime Environment) installed on the system
5. Start the server from a command line using the command `java -jar <name-of-jar-file>.jar`

## Development

This is a Clojure web service. It can be run locally using the leiningen command `lein run` if all dependencies are satisfied:
* JDK 9+
* [Leiningen](https://leiningen.org/)

For the server to start, it expects a file called properties.json in the root directory of the project.

In that file you can configure the port the server starts on (default 3000) and the secret token used to authenticate requests (default no authentication):

```
{
  "port" : 3000,
  "secret" : "secret"
}
```

I recommend using the [Cursive plugin](https://cursive-ide.com/) for [IntelliJ](https://www.jetbrains.com/idea/) for an IDE. However, [other options exist](https://jaxenter.com/top-5-ides-clojure-148368.html).