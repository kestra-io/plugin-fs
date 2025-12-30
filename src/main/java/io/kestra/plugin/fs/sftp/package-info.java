@PluginSubGroup(
    title = "SFTP (SSH File Transfer Protocol)",
    description = "This sub-group of plugins contains tasks for accessing files using the SFTP protocol.",
    categories = PluginSubGroup.PluginCategory.STORAGE,
    categories = {
        PluginSubGroup.PluginCategory.DATA,
        PluginSubGroup.PluginCategory.INFRASTRUCTURE
    }
)
package io.kestra.plugin.fs.sftp;

import io.kestra.core.models.annotations.PluginSubGroup;