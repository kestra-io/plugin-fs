package io.kestra.plugin.fs;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class UploadUtils {

    private UploadUtils() {
    }

    public static void validateUniqueDestinationFilenames(List<Map.Entry<String, String>> fileMappings) {
        validateUniqueDestinationFilenames(fileMappings, uri -> uri.substring(uri.lastIndexOf('/') + 1));
    }

    public static void validateUniqueDestinationFilenames(
        List<Map.Entry<String, String>> fileMappings,
        Function<String, String> nameExtractor
    ) {
        if (fileMappings == null) {
            return;
        }

        var destinationNames = new HashSet<String>();
        for (var entry : fileMappings) {
            var fileName = entry.getKey() != null
                ? entry.getKey()
                : nameExtractor.apply(entry.getValue());
            if (!destinationNames.add(fileName)) {
                throw new IllegalArgumentException(String.format(
                    "Multiple files resolve to the same destination filename '%s'. Set explicit unique target filenames in 'from' to avoid data loss.",
                    fileName
                ));
            }
        }
    }
}
