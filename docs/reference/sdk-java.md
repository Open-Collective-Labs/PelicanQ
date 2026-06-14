# Java SDK

## Installation

Add to your `pom.xml`:

```xml
<dependency>
  <groupId>com.pelicanq</groupId>
  <artifactId>pelicanq-client</artifactId>
  <version>0.1.0</version>
</dependency>
```

## Usage

```java
PelicanClient client = PelicanClient.forAddress("127.0.0.1", 7072).build();
// ...
```

## API

See `sdks/java/README.md` for complete documentation.
