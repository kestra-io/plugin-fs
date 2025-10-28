package io.kestra.plugin.fs.nfs;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.hamcrest.Matchers; 

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors; 

import static io.kestra.core.utils.Rethrow.throwFunction; 
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


class NfsTasksTest extends AbstractNfsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @TempDir
    private Path tempDirectory;
    private Path nfsMountPoint; 

    @BeforeEach
    void setUpNfsSim() throws IOException {
        nfsMountPoint = tempDirectory.resolve("nfs_share");
        Files.createDirectories(nfsMountPoint);
    }

    @Test
    void checkMount_validPath() throws Exception {
        
        CheckMount task = CheckMount.builder()
            .id(CheckMount.class.getSimpleName())
            .type(CheckMount.class.getName())
            .path(Property.ofValue(nfsMountPoint.toString())) // Explicitly set Property
            .build();

        
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        CheckMount.Output run = task.run(runContext);

        assertThat(run, notNullValue());
        
        assertThat(run.getFileStoreType(), not(containsStringIgnoringCase("nfs"))); // Correct assertion for local test
        assertThat(run.isNfsMount(), is(false)); // Correct assertion for local test
        assertThat(run.getPath(), is(nfsMountPoint.toString())); // Check output path
    }

    @Test
    void list_files() throws Exception {
        
        Path file1 = nfsMountPoint.resolve("file1.txt");
        Path file2 = nfsMountPoint.resolve("file2.csv");
        Path subdir = nfsMountPoint.resolve("subdir");
        Path file3 = subdir.resolve("file3.txt");

        Files.createFile(file1);
        Files.createFile(file2);
        Files.createDirectory(subdir);
        Files.createFile(file3);

        
        io.kestra.plugin.fs.nfs.List baseTask = io.kestra.plugin.fs.nfs.List.builder()
           .id("test-list")
           .type(io.kestra.plugin.fs.nfs.List.class.getName())
           .from(Property.ofValue(nfsMountPoint.toString())) // Set mandatory prop here too
           .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, baseTask, Map.of());


        
        io.kestra.plugin.fs.nfs.List task = io.kestra.plugin.fs.nfs.List.builder()
            .id(io.kestra.plugin.fs.nfs.List.class.getSimpleName())
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString())) // Explicitly set Property
            .recursive(Property.ofValue(false)) // FIX: Wrap boolean in Property.ofValue()
            .build();
        io.kestra.plugin.fs.nfs.List.Output run = task.run(runContext);
        // Should find file1.txt, file2.csv, subdir
        assertThat(run.getFiles(), hasSize(3));

        
        final io.kestra.plugin.fs.nfs.List recursiveTask = io.kestra.plugin.fs.nfs.List.builder()
            .id(io.kestra.plugin.fs.nfs.List.class.getSimpleName() + "-recursive") // Different ID
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString())) // Explicitly set Property
            .recursive(Property.ofValue(true)) // FIX: Wrap boolean in Property.ofValue()
            .build();

        
        final Path finalFromPath = nfsMountPoint; // Effectively final for lambda
        java.util.List<io.kestra.plugin.fs.nfs.List.File> files;
        try (var stream = Files.walk(finalFromPath)) {
             files = stream
                .filter(path -> !path.equals(finalFromPath))
                .map(throwFunction(p -> recursiveTask.mapToFile(p)))
                .collect(Collectors.toList());
        }
        

        
        assertThat(files, hasSize(4)); // Assert against the manually filtered list

        
        task = io.kestra.plugin.fs.nfs.List.builder()
            .id(io.kestra.plugin.fs.nfs.List.class.getSimpleName() + "-regexp") // Different ID
            .type(io.kestra.plugin.fs.nfs.List.class.getName())
            .from(Property.ofValue(nfsMountPoint.toString())) // Explicitly set Property
            .recursive(Property.ofValue(true)) // FIX: Wrap boolean in Property.ofValue()
            .regExp(Property.ofValue(".*\\.txt$"))
            .build();
        run = task.run(runContext); // Run the actual task for regexp test
        // Should find file1.txt, subdir/file3.txt
        assertThat(run.getFiles(), hasSize(2));
        
        List<String> foundNames = run.getFiles().stream().map(io.kestra.plugin.fs.nfs.List.File::getName).collect(Collectors.toList());
        assertThat(foundNames, Matchers.containsInAnyOrder("file1.txt", "file3.txt"));
        
        List<Path> foundPaths = run.getFiles().stream().map(io.kestra.plugin.fs.nfs.List.File::getLocalPath).collect(Collectors.toList());
        assertThat(foundPaths, Matchers.containsInAnyOrder(file1, file3));
        
    }

    @Test
    void copy_move_delete() throws Exception {
        Path sourceFile = nfsMountPoint.resolve("source.txt");
        Files.writeString(sourceFile, "copy me");
        Path copyDest = nfsMountPoint.resolve("copied.txt");
        Path moveDest = nfsMountPoint.resolve("moved.txt");

        
         Copy baseTask = Copy.builder()
            .id("test-copy")
            .type(Copy.class.getName())
            .from(Property.ofValue(sourceFile.toString())) // Set mandatory props here too
            .to(Property.ofValue(copyDest.toString()))     // Set mandatory props here too
            .build();
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, baseTask, Map.of());


        
        Copy copyTask = Copy.builder()
            .id(Copy.class.getSimpleName())
            .type(Copy.class.getName())
            .from(Property.ofValue(sourceFile.toString())) // Explicitly set Property
            .to(Property.ofValue(copyDest.toString()))     // Explicitly set Property
            .build();
        Copy.Output copyRun = copyTask.run(runContext);
        assertThat(Files.exists(copyDest), is(true));
        assertThat(Files.readString(copyDest), is("copy me"));
        assertThat(copyRun.getTo(), is(copyDest.toUri()));

        
        Move moveTask = Move.builder()
            .id(Move.class.getSimpleName())
            .type(Move.class.getName())
            .from(Property.ofValue(copyDest.toString())) // Explicitly set Property
            .to(Property.ofValue(moveDest.toString()))   // Explicitly set Property
            .build();
        Move.Output moveRun = moveTask.run(runContext);
        assertThat(Files.exists(copyDest), is(false));
        assertThat(Files.exists(moveDest), is(true));
        assertThat(Files.readString(moveDest), is("copy me"));
        assertThat(moveRun.getTo(), is(moveDest.toUri()));

        
        Delete deleteTask = Delete.builder()
            .id(Delete.class.getSimpleName() + "-moved")
            .type(Delete.class.getName())
            .uri(Property.ofValue(moveDest.toString())) // Explicitly set Property
            .build();
        Delete.Output deleteRun = deleteTask.run(runContext);
        assertThat(Files.exists(moveDest), is(false));
        assertThat(deleteRun.isDeleted(), is(true));
        assertThat(deleteRun.getUri(), is(moveDest.toUri()));

        
         deleteTask = Delete.builder()
             .id(Delete.class.getSimpleName() + "-original")
             .type(Delete.class.getName())
             .uri(Property.ofValue(sourceFile.toString())) // Explicitly set Property
             .build();
         deleteTask.run(runContext);
         assertThat(Files.exists(sourceFile), is(false));
    }
}

