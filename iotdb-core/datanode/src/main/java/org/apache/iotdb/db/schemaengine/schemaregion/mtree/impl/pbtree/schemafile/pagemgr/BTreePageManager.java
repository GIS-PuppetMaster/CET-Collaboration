/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.pagemgr;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.metadata.schemafile.ColossalRecordException;
import org.apache.iotdb.db.schemaengine.metric.SchemaRegionCachedMetric;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.mnode.ICachedMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISchemaPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.ISegmentedPage;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Queue;

import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile.getGlobalIndex;
import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile.getNodeAddress;
import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile.getPageIndex;
import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFile.getSegIndex;

public class BTreePageManager extends PageManager {

  public BTreePageManager(
      FileChannel channel,
      File pmtFile,
      int lastPageIndex,
      String logPath,
      SchemaRegionCachedMetric metric)
      throws IOException, MetadataException {
    super(channel, pmtFile, lastPageIndex, logPath, metric);
  }

  @Override
  protected void multiPageInsertOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer, SchemaPageContext cxt)
      throws MetadataException, IOException {
    // cur is a leaf, split and set next ptr as link between
    ISegmentedPage newPage = getMinApplSegmentedPageInMem(SchemaFileConfig.SEG_MAX_SIZ, cxt);
    newPage.allocNewSegment(SchemaFileConfig.SEG_MAX_SIZ);
    String sk =
        curPage
            .getAsSegmentedPage()
            .splitWrappedSegment(key, childBuffer, newPage, SchemaFileConfig.INCLINED_SPLIT);
    curPage
        .getAsSegmentedPage()
        .setNextSegAddress((short) 0, getGlobalIndex(newPage.getPageIndex(), (short) 0));
    cxt.markDirty(curPage);
    insertIndexEntryEntrance(curPage, newPage, sk, cxt);
  }

  @Override
  protected void multiPageUpdateOverflowOperation(
      ISchemaPage curPage, String key, ByteBuffer childBuffer, SchemaPageContext cxt)
      throws MetadataException, IOException {
    // even split and update higher nodes
    ISegmentedPage splPage = getMinApplSegmentedPageInMem(SchemaFileConfig.SEG_MAX_SIZ, cxt);
    splPage.allocNewSegment(SchemaFileConfig.SEG_MAX_SIZ);
    String sk = curPage.getAsSegmentedPage().splitWrappedSegment(null, null, splPage, false);
    curPage
        .getAsSegmentedPage()
        .setNextSegAddress((short) 0, getGlobalIndex(splPage.getPageIndex(), (short) 0));

    // update on page where it exists
    if (key.compareTo(sk) >= 0) {
      splPage.update((short) 0, key, childBuffer);
    } else {
      curPage.getAsSegmentedPage().update((short) 0, key, childBuffer);
    }

    // insert index entry upward
    insertIndexEntryEntrance(curPage, splPage, sk, cxt);
  }

  /**
   * In this implementation, subordinate index builds on alias.
   *
   * @param parNode node needs to build subordinate index.
   */
  @Override
  protected void buildSubIndex(ICachedMNode parNode, SchemaPageContext cxt)
      throws MetadataException, IOException {
    ISchemaPage cursorPage = getPageInstance(getPageIndex(getNodeAddress(parNode)), cxt);

    if (cursorPage.getAsInternalPage() == null) {
      throw new MetadataException("Subordinate index shall not build upon single page segment.");
    }

    ISchemaPage tPage = cursorPage; // reserve the top page to modify subIndex
    ISchemaPage subIndexPage =
        ISchemaPage.initAliasIndexPage(ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH), -1);
    registerAsNewPage(subIndexPage, cxt);

    // transfer cursorPage to leaf page
    while (cursorPage.getAsInternalPage() != null) {
      cursorPage =
          getPageInstance(getPageIndex(cursorPage.getAsInternalPage().getNextSegAddress()), cxt);
    }

    long nextAddr = cursorPage.getAsSegmentedPage().getNextSegAddress((short) 0);
    Queue<ICachedMNode> children = cursorPage.getAsSegmentedPage().getChildren((short) 0);
    ICachedMNode child;
    // TODO: inefficient to build B+Tree up-to-bottom, improve further
    while (!children.isEmpty() || nextAddr != -1L) {
      if (children.isEmpty()) {
        cursorPage = getPageInstance(getPageIndex(nextAddr), cxt);
        nextAddr = cursorPage.getAsSegmentedPage().getNextSegAddress((short) 0);
        children = cursorPage.getAsSegmentedPage().getChildren((short) 0);
      }
      child = children.poll();
      if (child != null
          && child.isMeasurement()
          && child.getAsMeasurementMNode().getAlias() != null) {
        subIndexPage =
            insertAliasIndexEntry(
                subIndexPage, child.getAsMeasurementMNode().getAlias(), child.getName(), cxt);
      }
    }

    tPage.setSubIndex(subIndexPage.getPageIndex());
  }

  @Override
  protected void insertSubIndexEntry(int base, String key, String rec, SchemaPageContext cxt)
      throws MetadataException, IOException {
    insertAliasIndexEntry(getPageInstance(base, cxt), key, rec, cxt);
  }

  @Override
  protected void removeSubIndexEntry(int base, String oldAlias, SchemaPageContext cxt)
      throws MetadataException, IOException {
    ISchemaPage tarPage = getTargetLeafPage(getPageInstance(base, cxt), oldAlias, cxt);
    tarPage.getAsAliasIndexPage().removeRecord(oldAlias);
  }

  @Override
  protected String searchSubIndexAlias(int base, String alias, SchemaPageContext cxt)
      throws MetadataException, IOException {
    return getTargetLeafPage(getPageInstance(base, cxt), alias, cxt)
        .getAsAliasIndexPage()
        .getRecordByAlias(alias);
  }

  /** @return top page to insert index */
  private ISchemaPage insertAliasIndexEntry(
      ISchemaPage topPage, String alias, String name, SchemaPageContext cxt)
      throws MetadataException, IOException {
    ISchemaPage tarPage = getTargetLeafPage(topPage, alias, cxt);
    if (tarPage.getAsAliasIndexPage() == null) {
      throw new MetadataException("File may be corrupted that subordinate index has broken.");
    }

    if (tarPage.getAsAliasIndexPage().insertRecord(alias, name) < 0) {
      // Need split and upwards insert
      ByteBuffer spltBuf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
      String sk =
          tarPage
              .getAsAliasIndexPage()
              .splitByKey(alias, name, spltBuf, SchemaFileConfig.INCLINED_SPLIT);
      ISchemaPage splPage = ISchemaPage.loadSchemaPage(spltBuf);
      registerAsNewPage(splPage, cxt);

      if (cxt.treeTrace[0] < 1) {
        // Transfer single sub-index page to tree structure
        ByteBuffer trsBuf = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
        tarPage.getAsAliasIndexPage().extendsTo(trsBuf);
        ISchemaPage trsPage = ISchemaPage.loadSchemaPage(trsBuf);

        // Notice that index of tarPage belongs to repPage then
        registerAsNewPage(trsPage, cxt);

        // tarPage abolished since then
        ISchemaPage repPage =
            ISchemaPage.initInternalPage(
                ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH),
                tarPage.getPageIndex(),
                trsPage.getPageIndex(),
                tarPage.getRefCnt(),
                tarPage.getLock());

        if (0 > repPage.getAsInternalPage().insertRecord(sk, splPage.getPageIndex())) {
          throw new ColossalRecordException(sk, alias);
        }

        repPage
            .getAsInternalPage()
            .setNextSegAddress(getGlobalIndex(trsPage.getPageIndex(), (short) 0));

        replacePageInCache(repPage, cxt);
        return repPage;
      } else {
        // no interleaved flush implemented since alias of the incoming records are NOT ordered
        insertIndexEntryRecursiveUpwards(cxt.treeTrace[0], sk, splPage.getPageIndex(), cxt);
        return getPageInstance(cxt.treeTrace[1], cxt);
      }
    }
    return topPage;
  }

  /**
   * Entrance to deal with main index structure of records.
   *
   * @param curPage original page occurred overflow.
   * @param splPage new page for original page to split.
   * @param sk least key of splPage.
   */
  private void insertIndexEntryEntrance(
      ISchemaPage curPage, ISchemaPage splPage, String sk, SchemaPageContext cxt)
      throws MetadataException, IOException {
    if (cxt.treeTrace[0] < 1) {
      // To make parent pointer valid after the btree established, curPage need to be transformed
      //  into an InternalPage. Since NOW page CANNOT transform in place, a substitute InternalPage
      //  inherits index from curPage initiated.
      ISegmentedPage trsPage = getMinApplSegmentedPageInMem(SchemaFileConfig.SEG_MAX_SIZ, cxt);
      trsPage.transplantSegment(
          curPage.getAsSegmentedPage(), (short) 0, SchemaFileConfig.SEG_MAX_SIZ);

      // repPage inherits lock and referent from curPage, which is always locked by entrant.
      ISchemaPage repPage =
          ISchemaPage.initInternalPage(
              ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH),
              curPage.getPageIndex(),
              trsPage.getPageIndex(),
              curPage.getRefCnt(),
              curPage.getLock());

      // link right child of the initiated InternalPage
      if (0 > repPage.getAsInternalPage().insertRecord(sk, splPage.getPageIndex())) {
        throw new ColossalRecordException(sk);
      }

      // setNextSegment of root as the left-most leaf
      repPage
          .getAsInternalPage()
          .setNextSegAddress(getGlobalIndex(trsPage.getPageIndex(), (short) 0));

      // mark the left-most child for interleaved flush, given the write operation is ordered
      cxt.invokeLastLeaf(trsPage);
      replacePageInCache(repPage, cxt);
    } else {
      // if the write starts from non-left-most leaf, it shall be recorded as well
      cxt.invokeLastLeaf(curPage);
      insertIndexEntryRecursiveUpwards(cxt.treeTrace[0], sk, splPage.getPageIndex(), cxt);
    }
  }

  /**
   * Insert an index entry into an internal page. Cascade insert or internal split conducted if
   * necessary.
   *
   * @param treeTraceIndex position of page index which had been traced
   * @param key key of the entry
   * @param ptr pointer of the entry
   */
  private void insertIndexEntryRecursiveUpwards(
      int treeTraceIndex, String key, int ptr, SchemaPageContext cxt)
      throws MetadataException, IOException {
    ISchemaPage idxPage = getPageInstance(cxt.treeTrace[treeTraceIndex], cxt);
    if (idxPage.getAsInternalPage().insertRecord(key, ptr) < 0) {
      // handle when insert an index entry occurring an overflow
      if (treeTraceIndex > 1) {
        // overflow, but existed parent to insert the split
        ByteBuffer dstBuffer = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
        String splitKey =
            idxPage
                .getAsInternalPage()
                .splitByKey(key, ptr, dstBuffer, SchemaFileConfig.INCLINED_SPLIT);
        ISchemaPage dstPage = ISchemaPage.loadSchemaPage(dstBuffer);
        registerAsNewPage(dstPage, cxt);
        insertIndexEntryRecursiveUpwards(treeTraceIndex - 1, splitKey, dstPage.getPageIndex(), cxt);
      } else {
        // treeTraceIndex==1, idxPage is the root of B+Tree, to split for new root internal
        ByteBuffer splBuffer = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);
        ByteBuffer trsBuffer = ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH);

        // idxPage splits and transplants, and remains to be root of B+Tree
        String splitKey =
            idxPage
                .getAsInternalPage()
                .splitByKey(key, ptr, splBuffer, SchemaFileConfig.INCLINED_SPLIT);
        idxPage.getAsInternalPage().extendsTo(trsBuffer);
        ISchemaPage splPage = ISchemaPage.loadSchemaPage(splBuffer);
        ISchemaPage trsPage = ISchemaPage.loadSchemaPage(trsBuffer);
        registerAsNewPage(splPage, cxt);
        registerAsNewPage(trsPage, cxt);

        idxPage.getAsInternalPage().resetBuffer(trsPage.getPageIndex());
        if (idxPage.getAsInternalPage().insertRecord(splitKey, splPage.getPageIndex()) < 0) {
          throw new ColossalRecordException(splitKey);
        }
        idxPage
            .getAsInternalPage()
            .setNextSegAddress(trsPage.getAsInternalPage().getNextSegAddress());
      }
    }
    cxt.markDirty(idxPage);
  }

  @Override
  public void delete(ICachedMNode node) throws IOException, MetadataException {
    cacheGuardian();
    SchemaPageContext cxt = new SchemaPageContext();
    // node is the record deleted from its segment
    entrantLock(node.getParent(), cxt);
    try {
      // remove corresponding record
      long recSegAddr = getNodeAddress(node.getParent());
      recSegAddr = getTargetSegmentAddress(recSegAddr, node.getName(), cxt);
      ISchemaPage tarPage = getPageInstance(getPageIndex(recSegAddr), cxt);
      cxt.markDirty(tarPage);
      tarPage.getAsSegmentedPage().removeRecord(getSegIndex(recSegAddr), node.getName());

      // remove segments belongs to node
      if (!node.isMeasurement() && getNodeAddress(node) > 0) {
        // node with maliciously modified address may result in orphan pages
        long delSegAddr = getNodeAddress(node);
        tarPage = getPageInstance(getPageIndex(delSegAddr), cxt);

        if (tarPage.getAsSegmentedPage() != null) {
          // TODO: may produce fractured page
          cxt.markDirty(tarPage);
          tarPage.getAsSegmentedPage().deleteSegment(getSegIndex(delSegAddr));
          if (tarPage.getAsSegmentedPage().validSegments() == 0) {
            tarPage.getAsSegmentedPage().purgeSegments();
          }
          cxt.indexBuckets.sortIntoBucket(tarPage, (short) -1);
        }

        if (tarPage.getAsInternalPage() != null) {
          // If the deleted one points to an Internal (root of BTree),
          //  there are two BTrees to handle:
          //  one mapping node names to record buffers,
          //  and another mapping aliases to names. <br>
          // All of those are turned into SegmentedPage.
          Deque<Integer> cascadePages =
              new ArrayDeque<>(tarPage.getAsInternalPage().getAllRecords());
          cascadePages.add(tarPage.getPageIndex());

          if (tarPage.getSubIndex() >= 0) {
            cascadePages.add(tarPage.getSubIndex());
          }

          while (!cascadePages.isEmpty()) {
            tarPage = getPageInstance(cascadePages.poll(), cxt);
            if (tarPage.getAsSegmentedPage() != null) {
              tarPage.getAsSegmentedPage().purgeSegments();
              cxt.markDirty(tarPage);
              cxt.indexBuckets.sortIntoBucket(tarPage, (short) -1);
              continue;
            }

            if (tarPage.getAsInternalPage() != null) {
              cascadePages.addAll(tarPage.getAsInternalPage().getAllRecords());
            }

            tarPage =
                ISchemaPage.initSegmentedPage(
                    ByteBuffer.allocate(SchemaFileConfig.PAGE_LENGTH),
                    tarPage.getPageIndex(),
                    tarPage.getRefCnt(),
                    tarPage.getLock());
            replacePageInCache(tarPage, cxt);
          }
        }
      }
      flushDirtyPages(cxt);
    } finally {
      releaseLocks(cxt);
      releaseReferent(cxt);
    }
  }

  /**
   * Since transplant and replacement may invalidate page instance held by a blocked read-thread,
   * the read-thread need to validate its page object once it obtained the lock.
   *
   * @param initPage page instance with read lock held
   * @param parent MTree node to read children, containing latest segment address
   * @param cxt update context if page/segment modified
   * @return validated page
   */
  private ISchemaPage validatePage(ISchemaPage initPage, ICachedMNode parent, SchemaPageContext cxt)
      throws IOException, MetadataException {
    boolean safeFlag = false;
    // check like crabbing
    ISchemaPage crabPage;
    SchemaPageContext doubleCheckContext;
    while (getPageIndex(getNodeAddress(parent)) != initPage.getPageIndex()) {
      // transplanted, release the stale and obtain/lock the new
      doubleCheckContext = new SchemaPageContext();
      long addrB4Lock = getNodeAddress(parent);
      int piB4Lock = getPageIndex(addrB4Lock);

      initPage.decrementAndGetRefCnt();
      initPage.getLock().readLock().unlock();

      crabPage = getPageInstance(piB4Lock, doubleCheckContext);
      crabPage.getLock().readLock().lock();

      // UNNECESSARY to TRACE lock since the very page will be unlocked at end of read
      cxt.referredPages.remove(initPage.getPageIndex());
      cxt.referredPages.put(crabPage.getPageIndex(), crabPage);
      initPage = crabPage;
    }

    // a fresh context to read-through from global cache
    doubleCheckContext = new SchemaPageContext();
    crabPage = getPageInstance(initPage.getPageIndex(), doubleCheckContext);
    if (crabPage != initPage) {
      // replaced, the lock and ref count should be the same
      if (crabPage.getLock() != initPage.getLock()
          || crabPage.getRefCnt() != initPage.getRefCnt()) {
        crabPage.decrementAndGetRefCnt();
        initPage.decrementAndGetRefCnt();
        initPage.getLock().readLock().unlock();
        throw new MetadataException(
            "Page[%d] replacement error: Different ref count or lock object.");
      }
      // update context is enough, ref and lock is left for main read process
      cxt.referredPages.put(initPage.getPageIndex(), crabPage);
      initPage = crabPage;
    }
    // same page shall only be referred once
    crabPage.decrementAndGetRefCnt();
    return initPage;
  }

  @Override
  public ICachedMNode getChildNode(ICachedMNode parent, String childName)
      throws MetadataException, IOException {
    SchemaPageContext cxt = new SchemaPageContext();
    threadContexts.put(Thread.currentThread().getId(), cxt);

    if (getNodeAddress(parent) < 0) {
      throw new MetadataException(
          String.format(
              "Node [%s] has no valid segment address in pbtree file.", parent.getFullPath()));
    }

    // a single read lock on initial page is sufficient to mutex write, no need to trace it
    int initIndex = getPageIndex(getNodeAddress(parent));
    ISchemaPage initPage = getPageInstance(initIndex, cxt);
    initPage.getLock().readLock().lock();
    initPage = validatePage(initPage, parent, cxt);
    try {
      long actualSegAddr = getTargetSegmentAddress(getNodeAddress(parent), childName, cxt);
      ICachedMNode child =
          getPageInstance(getPageIndex(actualSegAddr), cxt)
              .getAsSegmentedPage()
              .read(getSegIndex(actualSegAddr), childName);

      if (child == null && parent.isDevice()) {
        // try read alias directly first
        child =
            getPageInstance(getPageIndex(actualSegAddr), cxt)
                .getAsSegmentedPage()
                .readByAlias(getSegIndex(actualSegAddr), childName);
        if (child != null) {
          return child;
        }

        // try read with sub-index
        return getChildWithAlias(parent, childName, cxt);
      }
      return child;
    } finally {
      initPage.getLock().readLock().unlock();
      releaseReferent(cxt);
      threadContexts.remove(Thread.currentThread().getId(), cxt);
    }
  }

  private ICachedMNode getChildWithAlias(ICachedMNode par, String alias, SchemaPageContext cxt)
      throws IOException, MetadataException {
    long srtAddr = getNodeAddress(par);
    ISchemaPage page = getPageInstance(getPageIndex(srtAddr), cxt);

    if (page.getAsInternalPage() == null || page.getSubIndex() < 0) {
      return null;
    }

    String name = searchSubIndexAlias(page.getSubIndex(), alias, cxt);

    if (name == null) {
      return null;
    }

    return getTargetLeafPage(page, name, cxt).getAsSegmentedPage().read((short) 0, name);
  }

  @Override
  public Iterator<ICachedMNode> getChildren(ICachedMNode parent)
      throws MetadataException, IOException {
    SchemaPageContext cxt = new SchemaPageContext();
    int pageIdx = getPageIndex(getNodeAddress(parent));

    short segId = getSegIndex(getNodeAddress(parent));
    ISchemaPage page = getPageInstance(pageIdx, cxt), pageHeldLock;
    page.getLock().readLock().lock();
    page = validatePage(page, parent, cxt);
    pageHeldLock = page;

    try {
      while (page.getAsSegmentedPage() == null) {
        page = getPageInstance(getPageIndex(page.getAsInternalPage().getNextSegAddress()), cxt);
      }

      long actualSegAddr = page.getAsSegmentedPage().getNextSegAddress(segId);
      Queue<ICachedMNode> initChildren = page.getAsSegmentedPage().getChildren(segId);

      return new Iterator<ICachedMNode>() {
        long nextSeg = actualSegAddr;
        Queue<ICachedMNode> children = initChildren;

        @Override
        public boolean hasNext() {
          if (!children.isEmpty()) {
            return true;
          }
          if (nextSeg < 0) {
            return false;
          }

          try {
            ISchemaPage nPage;
            while (children.isEmpty() && nextSeg >= 0) {
              boolean hasThisPage = cxt.referredPages.containsKey(getPageIndex(nextSeg));
              nPage = getPageInstance(getPageIndex(nextSeg), cxt);
              children = nPage.getAsSegmentedPage().getChildren(getSegIndex(nextSeg));
              nextSeg = nPage.getAsSegmentedPage().getNextSegAddress(getSegIndex(nextSeg));
              // children iteration need not pin page, consistency is guaranteed by upper layer
              if (!hasThisPage) {
                cxt.referredPages.remove(nPage.getPageIndex());
                nPage.decrementAndGetRefCnt();
              }
            }
          } catch (MetadataException | IOException e) {
            logger.error(e.getMessage());
            return false;
          }

          return !children.isEmpty();
        }

        @Override
        public ICachedMNode next() {
          return children.poll();
        }
      };
    } finally {
      // safety of iterator should be guaranteed by upper layer
      pageHeldLock.getLock().readLock().unlock();
      releaseReferent(cxt);
    }
  }

  /** Seek non-InternalPage by name, syntax sugar of {@linkplain #getTargetSegmentAddress}. */
  private ISchemaPage getTargetLeafPage(ISchemaPage topPage, String recKey, SchemaPageContext cxt)
      throws IOException, MetadataException {
    cxt.treeTrace[0] = 0;
    if (topPage.getAsInternalPage() == null) {
      return topPage;
    }
    ISchemaPage curPage = topPage;

    int i = 0; // mark the trace of b+ tree node
    while (curPage.getAsInternalPage() != null) {
      i++;
      cxt.treeTrace[i] = curPage.getPageIndex();
      curPage = getPageInstance(curPage.getAsInternalPage().getRecordByKey(recKey), cxt);
    }
    cxt.treeTrace[0] = i; // bound in no.0 elem, points the parent the return

    return curPage;
  }

  /**
   * Search the segment contains target key with a B+Tree structure.
   *
   * @param curSegAddr address of the parent.
   * @param recKey target key.
   * @return address of the target segment.
   */
  @Override
  protected long getTargetSegmentAddress(long curSegAddr, String recKey, SchemaPageContext cxt)
      throws IOException, MetadataException {
    cxt.treeTrace[0] = 0;
    ISchemaPage curPage = getPageInstance(getPageIndex(curSegAddr), cxt);
    if (curPage.getAsSegmentedPage() != null) {
      return curSegAddr;
    }

    int i = 0; // mark the trace of b+ tree node
    while (curPage.getAsInternalPage() != null) {
      i++;
      cxt.treeTrace[i] = curPage.getPageIndex();
      curPage = getPageInstance(curPage.getAsInternalPage().getRecordByKey(recKey), cxt);
    }
    cxt.treeTrace[0] = i; // bound in no.0 elem, points the parent the return

    return getGlobalIndex(curPage.getPageIndex(), (short) 0);
  }
}
