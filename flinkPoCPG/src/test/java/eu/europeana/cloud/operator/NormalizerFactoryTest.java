package eu.europeana.cloud.operator;

import eu.europeana.normalization.Normalizer;
import eu.europeana.normalization.NormalizerFactory;
import eu.europeana.normalization.model.NormalizationResult;
import eu.europeana.normalization.util.NormalizationConfigurationException;
import eu.europeana.normalization.util.NormalizationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for normalization factory, the test could be copied to spring batch project, batch-normalization module to compare
 * behaviour on the spring batch POC library set. There were some small differences in result xml, printed in testNormalization()
 * There is also second
 */
class NormalizerFactoryTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizerFactoryTest.class);
  public static final int TRIES_COUNT = 200;
  public static final int THREAD_COUNT = 16;

  private NormalizerFactory normalizerFactory = new NormalizerFactory();

  @Test
  void testNormalization() throws NormalizationConfigurationException, NormalizationException {
    final Normalizer normalizer = normalizerFactory.getNormalizer();

    NormalizationResult normalizationResult = normalizer.normalize(SOURCE_RECORD);
    String result = normalizationResult.getNormalizedRecordInEdmXml();
    System.err.println("RESULT:");
    System.out.println(result);


  }

  @Test
  void testMultiThreadPerformance() throws InterruptedException {
    LOGGER.info("Starting test of normalization speed with {} threads and {} tries", THREAD_COUNT, TRIES_COUNT);
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT, r -> new Thread(r, "Background"));
    StopWatch watch = StopWatch.createStarted();
    AtomicInteger counter = new AtomicInteger(0);
    for (int i = 0; i < TRIES_COUNT; i++) {
      executor.submit(() -> {
        final Normalizer normalizer = normalizerFactory.getNormalizer();
        NormalizationResult normalizationResult = normalizer.normalize(SOURCE_RECORD);
        normalizationResult.getNormalizedRecordInEdmXml();
        counter.incrementAndGet();
        return null;
      });
    }
    LOGGER.info("Submitted!");
    executor.shutdown();
    while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {

      LOGGER.info("Progress: {}/{}, speed: {}, elapsed: {}", counter.get(), TRIES_COUNT, getSpeed(watch, counter),
          watch.formatTime());
    }
    LOGGER.info("Normalization test finished with a speed {}, in: {}", getSpeed(watch, counter), watch.formatTime());


  }

  private String getSpeed(StopWatch watch, AtomicInteger counter) {
    return "" + (counter.get() * 1000.0) / watch.getTime() + " records per second";
  }

  private static final String SOURCE_RECORD = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
      + "<rdf:RDF xmlns:adms=\"http://www.w3.org/ns/adms#\"\n"
      + "         xmlns:cc=\"http://creativecommons.org/ns#\"\n"
      + "         xmlns:crm=\"http://www.cidoc-crm.org/rdfs/cidoc_crm_v5.0.2_english_label.rdfs#\"\n"
      + "         xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n"
      + "         xmlns:dcat=\"http://www.w3.org/ns/dcat#\"\n"
      + "         xmlns:dcterms=\"http://purl.org/dc/terms/\"\n"
      + "         xmlns:doap=\"http://usefulinc.com/ns/doap#\"\n"
      + "         xmlns:ebucore=\"http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#\"\n"
      + "         xmlns:edm=\"http://www.europeana.eu/schemas/edm/\"\n"
      + "         xmlns:foaf=\"http://xmlns.com/foaf/0.1/\"\n"
      + "         xmlns:odrl=\"http://www.w3.org/ns/odrl/2/\"\n"
      + "         xmlns:ore=\"http://www.openarchives.org/ore/terms/\"\n"
      + "         xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n"
      + "         xmlns:rdaGr2=\"http://rdvocab.info/ElementsGr2/\"\n"
      + "         xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n"
      + "         xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"\n"
      + "         xmlns:skos=\"http://www.w3.org/2004/02/skos/core#\"\n"
      + "         xmlns:svcs=\"http://rdfs.org/sioc/services#\"\n"
      + "         xmlns:wgs84_pos=\"http://www.w3.org/2003/01/geo/wgs84_pos#\"\n"
      + "         xmlns:xs=\"http://www.w3.org/2001/XMLSchema\">\n"
      + "   <edm:ProvidedCHO rdf:about=\"/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\"/>\n"
      + "   <edm:WebResource rdf:about=\"https://heidicon.ub.uni-heidelberg.de/api/v1/objects/uuid/05b444c8-c47d-4b04-be2a-c86eaeffdb9e/file/id/221151/file_version/name/huge/disposition/inline\">\n"
      + "      <dc:format>image/jpeg</dc:format>\n"
      + "      <edm:rights rdf:resource=\"http://rightsstatements.org/vocab/InC/1.0/\"/>\n"
      + "   </edm:WebResource>\n"
      + "   <skos:Concept rdf:about=\"https://d-nb.info/gnd/4029670-2\">\n"
      + "      <skos:prefLabel>Karikatur</skos:prefLabel>\n"
      + "   </skos:Concept>\n"
      + "   <skos:Concept rdf:about=\"https://d-nb.info/gnd/4135144-7\">\n"
      + "      <skos:prefLabel>Satirische Zeitschrift</skos:prefLabel>\n"
      + "   </skos:Concept>\n"
      + "   <ore:Aggregation rdf:about=\"/aggregation/provider/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\">\n"
      + "      <edm:aggregatedCHO rdf:resource=\"/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\"/>\n"
      + "      <edm:dataProvider>Ruprecht-Karls-Universit�t Heidelberg. Universit�tsbibliothek</edm:dataProvider>\n"
      + "      <edm:isShownAt rdf:resource=\"https://heidicon.ub.uni-heidelberg.de/detail/750097\"/>\n"
      + "      <edm:isShownBy rdf:resource=\"https://heidicon.ub.uni-heidelberg.de/api/v1/objects/uuid/05b444c8-c47d-4b04-be2a-c86eaeffdb9e/file/id/221151/file_version/name/huge/disposition/inline\"/>\n"
      + "      <edm:object rdf:resource=\"https://heidicon.ub.uni-heidelberg.de/api/v1/objects/uuid/05b444c8-c47d-4b04-be2a-c86eaeffdb9e/file/id/221151/file_version/name/huge/disposition/inline\"/>\n"
      + "      <edm:provider>Deutsche Digitale Bibliothek</edm:provider>\n"
      + "      <edm:rights rdf:resource=\"http://rightsstatements.org/vocab/InC/1.0/\"/>\n"
      + "      <edm:intermediateProvider/>\n"
      + "   </ore:Aggregation>\n"
      + "   <ore:Proxy rdf:about=\"/proxy/provider/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\">\n"
      + "      <dc:date>1910 (Herstellung)</dc:date>\n"
      + "      <dc:identifier>G 5442-2 Folio RES (Inventarnummer)</dc:identifier>\n"
      + "      <dc:relation xml:lang=\"ger\">Verweis: http://digi.ub.uni-heidelberg.de/diglit/fb134/0250</dc:relation>\n"
      + "      <dc:subject rdf:resource=\"https://d-nb.info/gnd/4029670-2\"/>\n"
      + "      <dc:subject rdf:resource=\"https://d-nb.info/gnd/4135144-7\"/>\n"
      + "      <dc:title xml:lang=\"ger\">\"Auf der Hofjagd\"</dc:title>\n"
      + "      <dc:type rdf:resource=\"https://d-nb.info/gnd/4021845-4\"/>\n"
      + "      <dcterms:alternative xml:lang=\"ger\">Fliegende Bl�tter (Serientitel)</dcterms:alternative>\n"
      + "      <dcterms:provenance xml:lang=\"ger\">Universit�tsbibliothek Heidelberg</dcterms:provenance>\n"
      + "      <dcterms:spatial xml:lang=\"ger\">M�nchen (Herstellung)</dcterms:spatial>\n"
      + "      <edm:europeanaProxy>false</edm:europeanaProxy>\n"
      + "      <ore:proxyFor rdf:resource=\"/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\"/>\n"
      + "      <ore:proxyIn rdf:resource=\"/aggregation/provider/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\"/>\n"
      + "      <edm:type>IMAGE</edm:type>\n"
      + "   </ore:Proxy>\n"
      + "   <ore:Proxy rdf:about=\"/proxy/europeana/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\">\n"
      + "      <dc:identifier>http://www.deutsche-digitale-bibliothek.de/item/2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO</dc:identifier>\n"
      + "      <edm:europeanaProxy>true</edm:europeanaProxy>\n"
      + "      <ore:proxyFor rdf:resource=\"/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\"/>\n"
      + "      <ore:proxyIn rdf:resource=\"/aggregation/europeana/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\"/>\n"
      + "      <ore:lineage rdf:resource=\"/proxy/provider/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\"/>\n"
      + "   </ore:Proxy>\n"
      + "   <edm:EuropeanaAggregation rdf:about=\"/aggregation/europeana/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\">\n"
      + "      <edm:aggregatedCHO rdf:resource=\"/JUni/item_2A3LYMU57JAGNYDIW5GI3TJYHQFEP4HO\"/>\n"
      + "      <edm:dataProvider xml:lang=\"en\">Europeana Foundation</edm:dataProvider>\n"
      + "      <edm:provider xml:lang=\"en\">Europeana Foundation</edm:provider>\n"
      + "      <edm:datasetName>idA_metisDatasetNameA</edm:datasetName>\n"
      + "      <edm:country>Greece</edm:country>\n"
      + "      <edm:language>el</edm:language>\n"
      + "   </edm:EuropeanaAggregation>\n"
      + "</rdf:RDF>\n";

}

