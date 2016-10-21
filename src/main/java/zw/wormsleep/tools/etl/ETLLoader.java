package zw.wormsleep.tools.etl;


public interface ETLLoader {
    void load(ETLExtractor extractor, ETLTransformer transformer);
}
