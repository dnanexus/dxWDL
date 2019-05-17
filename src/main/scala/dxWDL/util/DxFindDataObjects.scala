/*

In DxNI:
                DXSearch.findDataObjects()
                    .inFolderOrSubfolders(dxProject, folder)
                    .withClassApplet
                    .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
                    .execute().asList().asScala.toVector


DxObjectDirectory
        val dxObjectsInFolder: List[DXDataObject] = DXSearch.findDataObjects()
            .inFolder(dxProject, folder)
            .withVisibility(DXSearch.VisibilityQuery.EITHER)
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList

        val dxAppletsInProject: List[DXDataObject] = DXSearch.findDataObjects()
            .inProject(dxProject)
            .withVisibility(DXSearch.VisibilityQuery.EITHER)
            .withProperty(CHECKSUM_PROP)
            .withClassApplet
            .includeDescribeOutput(DXDataObject.DescribeOptions.get().withProperties())
            .execute().asList().asScala.toList

WorkflowOutputReorg
    def bulkGetFilenames(files: Seq[DXFile], dxProject: DXProject) : Vector[String] = {
        val info:List[DXDataObject] = DXSearch.findDataObjects()
            .withIdsIn(files.asJava)
            .inProject(dxProject)
            .includeDescribeOutput()
            .execute().asList().asScala.toList
        info.map(_.getCachedDescribe().getName()).toVector
    }

 */
