//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FUSE_IPP
#define LLFS_FUSE_IPP

#include <llfs/api_types.hpp>

#include <batteries/slice.hpp>
#include <batteries/suppress.hpp>

namespace llfs {

/**
 * Initialize filesystem
 *
 * This function is called when libfuse establishes
 * communication with the FUSE kernel module. The file system
 * should use this module to inspect and/or modify the
 * connection parameters provided in the `conn` structure.
 *
 * Note that some parameters may be overwritten by options
 * passed to fuse_session_new() which take precedence over the
 * values set in this handler.
 *
 * There's no reply to this function
 *
 * @param userdata the user data passed to fuse_session_new()
 */ // 1/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_init_impl(void* userdata, struct fuse_conn_info* conn)
{
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(impl);

  impl->conn_ = conn;
  impl->derived_this()->init();
}

/**
 * Clean up filesystem.
 *
 * Called on filesystem exit. When this method is called, the
 * connection to the kernel may be gone already, so that eg. calls
 * to fuse_lowlevel_notify_* will fail.
 *
 * There's no reply to this function
 *
 * @param userdata the user data passed to fuse_session_new()
 */ // 2/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_destroy_impl(void* userdata)
{
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(impl);

  auto on_scope_exit = batt::finally([&] {
    impl->conn_ = nullptr;
  });

  impl->derived_this()->destroy();
}

/**
 * Look up a directory entry by name and get its attributes.
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name the name to look up
 */ // 3/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_lookup_impl(fuse_req_t req, fuse_ino_t parent,
                                                         const char* name)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_lookup(req, parent, name, impl->make_entry_handler(req));
}

/**
 * Forget about an inode
 *
 * This function is called when the kernel removes an inode
 * from its internal caches.
 *
 * The inode's lookup count increases by one for every call to
 * fuse_reply_entry and fuse_reply_create. The nlookup parameter
 * indicates by how much the lookup count should be decreased.
 *
 * Inodes with a non-zero lookup count may receive request from
 * the kernel even after calls to unlink, rmdir or (when
 * overwriting an existing file) rename. Filesystems must handle
 * such requests properly and it is recommended to defer removal
 * of the inode until the lookup count reaches zero. Calls to
 * unlink, rmdir or rename will be followed closely by forget
 * unless the file or directory is open, in which case the
 * kernel issues forget only after the release or releasedir
 * calls.
 *
 * Note that if a file system will be exported over NFS the
 * inodes lifetime must extend even beyond forget. See the
 * generation field in struct fuse_entry_param above.
 *
 * On unmount the lookup count for all inodes implicitly drops
 * to zero. It is not guaranteed that the file system will
 * receive corresponding forget messages for the affected
 * inodes.
 *
 * Valid replies:
 *   fuse_reply_none
 *
 * @param req request handle
 * @param ino the inode number
 * @param nlookup the number of lookups to forget
 */ // 4/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_forget_impl(fuse_req_t req, fuse_ino_t ino,
                                                         uint64_t nlookup)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_forget_inode(req, ino, nlookup, impl->make_no_arg_handler(req));
}

/**
 * Get file attributes.
 *
 * If writeback caching is enabled, the kernel may have a
 * better idea of a file's length than the FUSE file system
 * (eg if there has been a write that extended the file size,
 * but that has not yet been passed to the filesystem.n
 *
 * In this case, the st_size value provided by the file system
 * will be ignored.
 *
 * Valid replies:
 *   fuse_reply_attr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi for future use, currently always NULL
 */ // 5/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_getattr_impl(fuse_req_t req, fuse_ino_t ino,
                                                          struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  if (fi != nullptr) {
    LLFS_LOG_WARNING() << "(getattr) Expected fi to be NULL!";
  }

  impl->derived_this()->async_get_attributes(req, ino, impl->make_attributes_handler(req));
}

/**
 * Set file attributes
 *
 * In the 'attr' argument only members indicated by the 'to_set'
 * bitmask contain valid values.  Other members contain undefined
 * values.
 *
 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
 * expected to reset the setuid and setgid bits if the file
 * size or owner is being changed.
 *
 * This method will not be called to update st_atime or st_ctime implicitly
 * (eg. after a read() request), and only be called to implicitly update st_mtime
 * if writeback caching is active. It is the filesystem's responsibility to update
 * these timestamps when needed, and (if desired) to implement mount options like
 * `noatime` or `relatime`.
 *
 * If the setattr was invoked from the ftruncate() system call
 * under Linux kernel versions 2.6.15 or later, the fi->fh will
 * contain the value set by the open method or will be undefined
 * if the open method didn't set any value.  Otherwise (not
 * ftruncate call, or kernel version earlier than 2.6.15) the fi
 * parameter will be NULL.
 *
 * Valid replies:
 *   fuse_reply_attr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param attr the attributes
 * @param to_set bit mask of attributes which should be set
 * @param fi file information, or NULL
 */ // 6/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_setattr_impl(fuse_req_t req, fuse_ino_t ino,
                                                          struct stat* attr, int to_set,
                                                          struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_set_attributes(req, ino, attr, to_set, fi,
                                             impl->make_attributes_handler(req));
}

/**
 * Read symbolic link
 *
 * Valid replies:
 *   fuse_reply_readlink
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 */ // 7/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_readlink_impl(fuse_req_t req, fuse_ino_t ino)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_readlink(req, ino, impl->make_readlink_handler(req));
}

/**
 * Create file node
 *
 * Create a regular file, character device, block device, fifo or
 * socket node.
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode file type and mode with which to create the new file
 * @param rdev the device number (only valid if created file is a device)
 */ // 8/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_mknod_impl(fuse_req_t req, fuse_ino_t parent,
                                                        const char* name, mode_t mode, dev_t rdev)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_make_node(req, parent, name, mode, rdev,
                                        impl->make_entry_handler(req));
}

/**
 * Create a directory
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode with which to create the new file
 */ // 9/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_mkdir_impl(fuse_req_t req, fuse_ino_t parent,
                                                        const char* name, mode_t mode)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_make_directory(req, parent, name, mode,
                                             impl->make_entry_handler(req));
}

/**
 * Remove a file
 *
 * If the file's inode's lookup count is non-zero, the file
 * system is expected to postpone any removal of the inode
 * until the lookup count reaches zero (see description of the
 * forget function).
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to remove
 */ // 10/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_unlink_impl(fuse_req_t req, fuse_ino_t parent,
                                                         const char* name)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_unlink(req, parent, name, impl->make_error_handler(req));
}

/**
 * Remove a directory
 *
 * If the directory's inode's lookup count is non-zero, the
 * file system is expected to postpone any removal of the
 * inode until the lookup count reaches zero (see description
 * of the forget function).
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to remove
 */ // 11/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_rmdir_impl(fuse_req_t req, fuse_ino_t parent,
                                                        const char* name)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_remove_directory(req, parent, name, impl->make_error_handler(req));
}

/**
 * Create a symbolic link
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param link the contents of the symbolic link
 * @param parent inode number of the parent directory
 * @param name to create
 */ // 12/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_symlink_impl(fuse_req_t req, const char* link,
                                                          fuse_ino_t parent, const char* name)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_symbolic_link(req, link, parent, name, impl->make_entry_handler(req));
}

/** Rename a file
 *
 * If the target exists it should be atomically replaced. If
 * the target's inode's lookup count is non-zero, the file
 * system is expected to postpone any removal of the inode
 * until the lookup count reaches zero (see description of the
 * forget function).
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EINVAL, i.e. all
 * future bmap requests will fail with EINVAL without being
 * send to the filesystem process.
 *
 * *flags* may be `RENAME_EXCHANGE` or `RENAME_NOREPLACE`. If
 * RENAME_NOREPLACE is specified, the filesystem must not
 * overwrite *newname* if it exists and return an error
 * instead. If `RENAME_EXCHANGE` is specified, the filesystem
 * must atomically exchange the two files, i.e. both must
 * exist and neither may be deleted.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the old parent directory
 * @param name old name
 * @param newparent inode number of the new parent directory
 * @param newname new name
 */ // 13/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_rename_impl(fuse_req_t req, fuse_ino_t parent,
                                                         const char* name, fuse_ino_t newparent,
                                                         const char* newname, unsigned int flags)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_rename(req, parent, name, newparent, newname, flags,
                                     impl->make_error_handler(req));
}

/**
 * Create a hard link
 *
 * Valid replies:
 *   fuse_reply_entry
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the old inode number
 * @param newparent inode number of the new parent directory
 * @param newname new name to create
 */ // 14/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_link_impl(fuse_req_t req, fuse_ino_t ino,
                                                       fuse_ino_t newparent, const char* newname)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_hard_link(req, ino, newparent, newname,
                                        impl->make_entry_handler(req));
}

/**
 * Open a file
 *
 * Open flags are available in fi->flags. The following rules
 * apply.
 *
 *  - Creation (O_CREAT, O_EXCL, O_NOCTTY) flags will be
 *    filtered out / handled by the kernel.
 *
 *  - Access modes (O_RDONLY, O_WRONLY, O_RDWR) should be used
 *    by the filesystem to check if the operation is
 *    permitted.  If the ``-o default_permissions`` mount
 *    option is given, this check is already done by the
 *    kernel before calling open() and may thus be omitted by
 *    the filesystem.
 *
 *  - When writeback caching is enabled, the kernel may send
 *    read requests even for files opened with O_WRONLY. The
 *    filesystem should be prepared to handle this.
 *
 *  - When writeback caching is disabled, the filesystem is
 *    expected to properly handle the O_APPEND flag and ensure
 *    that each write is appending to the end of the file.
 *
 *  - When writeback caching is enabled, the kernel will
 *    handle O_APPEND. However, unless all changes to the file
 *    come through the kernel this will not work reliably. The
 *    filesystem should thus either ignore the O_APPEND flag
 *    (and let the kernel handle it), or return an error
 *    (indicating that reliably O_APPEND is not available).
 *
 * Filesystem may store an arbitrary file handle (pointer,
 * index, etc) in fi->fh, and use this in other all other file
 * operations (read, write, flush, release, fsync).
 *
 * Filesystem may also implement stateless file I/O and not store
 * anything in fi->fh.
 *
 * There are also some flags (direct_io, keep_cache) which the
 * filesystem may set in fi, to change the way the file is opened.
 * See fuse_file_info structure in <fuse_common.h> for more details.
 *
 * If this request is answered with an error code of ENOSYS
 * and FUSE_CAP_NO_OPEN_SUPPORT is set in
 * `fuse_conn_info.capable`, this is treated as success and
 * future calls to open and release will also succeed without being
 * sent to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_open
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */ // 15/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_open_impl(fuse_req_t req, fuse_ino_t ino,
                                                       struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_open(req, ino, fi, impl->make_open_handler(req));
}

/**
 * Read data
 *
 * Read should send exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the file
 * has been opened in 'direct_io' mode, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_iov
 *   fuse_reply_data
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size number of bytes to read
 * @param off offset to read from
 * @param fi file information
 */ // 16/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_read_impl(fuse_req_t req, fuse_ino_t ino, size_t size,
                                                       off_t off, struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  impl->derived_this()->async_read(req, ino, size, FileOffset{off}, FuseFileHandle{fi->fh},
                                   impl->make_read_handler(req));
}

/**
 * Write data
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the file has
 * been opened in 'direct_io' mode, in which case the return value
 * of the write system call will reflect the return value of this
 * operation.
 *
 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
 * expected to reset the setuid and setgid bits.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param buf data to write
 * @param size number of bytes to write
 * @param off offset to write to
 * @param fi file information
 */ // 17/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_write_impl(fuse_req_t req, fuse_ino_t ino,
                                                        const char* buf, size_t size, off_t off,
                                                        struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  impl->derived_this()->async_write(req, ino, batt::ConstBuffer{buf, size}, FileOffset{off},
                                    FuseFileHandle{fi->fh}, impl->make_write_handler(req));
}

/**
 * Flush method
 *
 * This is called on each close() of the opened file.
 *
 * Since file descriptors can be duplicated (dup, dup2, fork), for
 * one open call there may be many flush calls.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * NOTE: the name of the method is misleading, since (unlike
 * fsync) the filesystem is not forced to flush pending writes.
 * One reason to flush data is if the filesystem wants to return
 * write errors during close.  However, such use is non-portable
 * because POSIX does not require [close] to wait for delayed I/O to
 * complete.
 *
 * If the filesystem supports file locking operations (setlk,
 * getlk) it should remove all locks belonging to 'fi->owner'.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to flush() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 *
 * [close]: http://pubs.opengroup.org/onlinepubs/9699919799/functions/close.html
 */ // 18/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_flush_impl(fuse_req_t req, fuse_ino_t ino,
                                                        struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  /* TODO [tastolfi 2023-07-14] Take into account:
   *
   * "If the filesystem supports file locking operations (setlk,
   *  getlk) it should remove all locks belonging to 'fi->owner'."
   */

  impl->derived_this()->async_flush(req, ino, FuseFileHandle{fi->fh},
                                    impl->make_error_handler(req));
}

/**
 * Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open call there will be exactly one release call (unless
 * the filesystem is force-unmounted).
 *
 * The filesystem may reply with an error, but error values are
 * not returned to close() or munmap() which triggered the
 * release.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 * fi->flags will contain the same flags as for open.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */ // 19/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_release_impl(fuse_req_t req, fuse_ino_t ino,
                                                          struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  impl->derived_this()->async_release(req, ino, FuseFileHandle{fi->fh}, FileOpenFlags{fi->flags},
                                      impl->make_error_handler(req));
}

/**
 * Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to fsync() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */ // 20/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_fsync_impl(fuse_req_t req, fuse_ino_t ino,
                                                        int datasync, struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  impl->derived_this()->async_fsync(req, ino, IsDataSync{datasync != 0}, FuseFileHandle{fi->fh},
                                    impl->make_error_handler(req));
}

/**
 * Open a directory
 *
 * Filesystem may store an arbitrary file handle (pointer, index,
 * etc) in fi->fh, and use this in other all other directory
 * stream operations (readdir, releasedir, fsyncdir).
 *
 * If this request is answered with an error code of ENOSYS and
 * FUSE_CAP_NO_OPENDIR_SUPPORT is set in `fuse_conn_info.capable`,
 * this is treated as success and future calls to opendir and
 * releasedir will also succeed without being sent to the filesystem
 * process. In addition, the kernel will cache readdir results
 * as if opendir returned FOPEN_KEEP_CACHE | FOPEN_CACHE_DIR.
 *
 * Valid replies:
 *   fuse_reply_open
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */ // 21/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_opendir_impl(fuse_req_t req, fuse_ino_t ino,
                                                          struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_opendir(req, ino, fi, impl->make_open_handler(req));
}

/**
 * Read directory
 *
 * Send a buffer filled using fuse_add_direntry(), with size not
 * exceeding the requested size.  Send an empty buffer on end of
 * stream.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * Returning a directory entry from readdir() does not affect
 * its lookup count.
 *
 * If off_t is non-zero, then it will correspond to one of the off_t
 * values that was previously returned by readdir() for the same
 * directory handle. In this case, readdir() should skip over entries
 * coming before the position defined by the off_t value. If entries
 * are added or removed while the directory handle is open, the filesystem
 * may still include the entries that have been removed, and may not
 * report the entries that have been created. However, addition or
 * removal of entries must never cause readdir() to skip over unrelated
 * entries or to report them more than once. This means
 * that off_t can not be a simple index that enumerates the entries
 * that have been returned but must contain sufficient information to
 * uniquely determine the next directory entry to return even when the
 * set of entries is changing.
 *
 * The function does not have to report the '.' and '..'
 * entries, but is allowed to do so. Note that, if readdir does
 * not return '.' or '..', they will not be implicitly returned,
 * and this behavior is observable by the caller.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size maximum number of bytes to send
 * @param off offset to continue reading the directory stream
 * @param fi file information
 */ // 22/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_readdir_impl(fuse_req_t req, fuse_ino_t ino,
                                                          size_t size, off_t off,
                                                          struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  impl->derived_this()->async_readdir(req, ino, size, DirentOffset{off}, FuseFileHandle{fi->fh},
                                      impl->make_readdir_handler(req));
}

/**
 * Release an open directory
 *
 * For every opendir call there will be exactly one releasedir
 * call (unless the filesystem is force-unmounted).
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 */ // 23/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_releasedir_impl(fuse_req_t req, fuse_ino_t ino,
                                                             struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  impl->derived_this()->async_releasedir(req, ino, FuseFileHandle{fi->fh},
                                         impl->make_error_handler(req));
}

/**
 * Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the directory
 * contents should be flushed, not the meta data.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * If this request is answered with an error code of ENOSYS,
 * this is treated as success and future calls to fsyncdir() will
 * succeed automatically without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param datasync flag indicating if only data should be flushed
 * @param fi file information
 */ // 24/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_fsyncdir_impl(fuse_req_t req, fuse_ino_t ino,
                                                           int datasync, struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  impl->derived_this()->async_fsyncdir(req, ino, IsDataSync{datasync != 0}, FuseFileHandle{fi->fh},
                                       impl->make_error_handler(req));
}

/**
 * Get file system statistics
 *
 * Valid replies:
 *   fuse_reply_statfs
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number, zero means "undefined"
 */ // 25/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_statfs_impl(fuse_req_t req, fuse_ino_t ino)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_statfs(req, ino, impl->make_statfs_handler(req));
}

/**
 * Set an extended attribute
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future setxattr() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_err
 */ // 26/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_setxattr_impl(fuse_req_t req, fuse_ino_t ino,
                                                           const char* name, const char* value,
                                                           size_t size, int flags)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_set_extended_attribute(req, ino,
                                                     FuseImplBase::ExtendedAttribute{
                                                         .name = std::string_view{name},
                                                         .value = batt::ConstBuffer{value, size},
                                                     },
                                                     flags, impl->make_error_handler(req));
}

/**
 * Get an extended attribute
 *
 * If size is zero, the size of the value should be sent with
 * fuse_reply_xattr.
 *
 * If the size is non-zero, and the value fits in the buffer, the
 * value should be sent with fuse_reply_buf.
 *
 * If the size is too small for the value, the ERANGE error should
 * be sent.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future getxattr() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_xattr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param name of the extended attribute
 * @param size maximum size of the value to send
 */ // 27/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_getxattr_impl(fuse_req_t req, fuse_ino_t ino,
                                                           const char* name, size_t size)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_get_extended_attribute(req, ino, name, size,
                                                     impl->make_extended_attribute_handler(req));
}

/**
 * List extended attribute names
 *
 * If size is zero, the total size of the attribute list should be
 * sent with fuse_reply_xattr.
 *
 * If the size is non-zero, and the null character separated
 * attribute list fits in the buffer, the list should be sent with
 * fuse_reply_buf.
 *
 * If the size is too small for the list, the ERANGE error should
 * be sent.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future listxattr() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_xattr
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size maximum size of the list to send
 */ // 28/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_listxattr_impl(fuse_req_t req, fuse_ino_t ino,
                                                            size_t size)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  (void)ino;
  (void)size;

  LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;
  // TODO [tastolfi 2023-06-28]

  fuse_reply_err(req,
                 FuseImplBase::errno_from_status(batt::Status{batt::StatusCode::kUnimplemented}));
}

/**
 * Remove an extended attribute
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future removexattr() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param name of the extended attribute
 */ // 29/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_removexattr_impl(fuse_req_t req, fuse_ino_t ino,
                                                              const char* name)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_remove_extended_attribute(req, ino, name,
                                                        impl->make_error_handler(req));
}

/**
 * Check file access permissions
 *
 * This will be called for the access() and chdir() system
 * calls.  If the 'default_permissions' mount option is given,
 * this method is not called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent success, i.e. this and all future access()
 * requests will succeed without being send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param mask requested access mode
 */ // 30/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_access_impl(fuse_req_t req, fuse_ino_t ino, int mask)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_check_access(req, ino, mask, impl->make_error_handler(req));
}

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * See the description of the open handler for more
 * information.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * If this request is answered with an error code of ENOSYS, the handler
 * is treated as not implemented (i.e., for this and future requests the
 * mknod() and open() handlers will be called instead).
 *
 * Valid replies:
 *   fuse_reply_create
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the parent directory
 * @param name to create
 * @param mode file type and mode with which to create the new file
 * @param fi file information
 */ // 31/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_create_impl(fuse_req_t req, fuse_ino_t parent,
                                                         const char* name, mode_t mode,
                                                         struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_create(req, parent, name, mode, fi, impl->make_create_handler(req));
}

/**
 * Test for a POSIX file lock
 *
 * Valid replies:
 *   fuse_reply_lock
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param lock the region/type to test
 */ // 32/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_getlk_impl(fuse_req_t req, fuse_ino_t ino,
                                                        struct fuse_file_info* fi,
                                                        struct flock* lock)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  (void)ino;
  (void)fi;
  (void)lock;

  LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;
  // TODO [tastolfi 2023-06-28]

  fuse_reply_err(req,
                 FuseImplBase::errno_from_status(batt::Status{batt::StatusCode::kUnimplemented}));
}

/**
 * Acquire, modify or release a POSIX file lock
 *
 * For POSIX threads (NPTL) there's a 1-1 relation between pid and
 * owner, but otherwise this is not always the case.  For checking
 * lock ownership, 'fi->owner' must be used.  The l_pid field in
 * 'struct flock' should only be used to fill in this field in
 * getlk().
 *
 * Note: if the locking methods are not implemented, the kernel
 * will still allow file locking to work locally.  Hence these are
 * only interesting for network filesystems and similar.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param lock the region/type to set
 * @param sleep locking operation may sleep
 */ // 33/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_setlk_impl(fuse_req_t req, fuse_ino_t ino,
                                                        struct fuse_file_info* fi,
                                                        struct flock* lock, int sleep)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  (void)ino;
  (void)fi;
  (void)lock;
  (void)sleep;

  LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;
  // TODO [tastolfi 2023-06-28]

  fuse_reply_err(req,
                 FuseImplBase::errno_from_status(batt::Status{batt::StatusCode::kUnimplemented}));
}

/**
 * Map block index within file to block index within device
 *
 * Note: This makes sense only for block device backed filesystems
 * mounted with the 'blkdev' option
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure, i.e. all future bmap() requests will
 * fail with the same error code without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_bmap
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param blocksize unit of block index
 * @param idx block index within file
 */ // 34/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_bmap_impl(fuse_req_t req, fuse_ino_t ino,
                                                       size_t blocksize, uint64_t idx)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  (void)ino;
  (void)blocksize;
  (void)idx;

  LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;
  // TODO [tastolfi 2023-06-28]

  fuse_reply_err(req,
                 FuseImplBase::errno_from_status(batt::Status{batt::StatusCode::kUnimplemented}));
}

/**
 * Ioctl
 *
 * Note: For unrestricted ioctls (not allowed for FUSE
 * servers), data in and out areas can be discovered by giving
 * iovs and setting FUSE_IOCTL_RETRY in *flags*.  For
 * restricted ioctls, kernel prepares in/out data area
 * according to the information encoded in cmd.
 *
 * Valid replies:
 *   fuse_reply_ioctl_retry
 *   fuse_reply_ioctl
 *   fuse_reply_ioctl_iov
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param cmd ioctl command
 * @param arg ioctl argument
 * @param fi file information
 * @param flags for FUSE_IOCTL_* flags
 * @param in_buf data fetched from the caller
 * @param in_bufsz number of fetched bytes
 * @param out_bufsz maximum size of output data
 *
 * Note : the unsigned long request submitted by the application
 * is truncated to 32 bits.
 */ // 35/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_ioctl_impl(fuse_req_t req, fuse_ino_t ino,
                                                        unsigned int cmd, void* arg,
                                                        struct fuse_file_info* fi, unsigned flags,
                                                        const void* in_buf, size_t in_bufsz,
                                                        size_t out_bufsz)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_ioctl(req, ino, cmd, arg, fi, flags,
                                    /*in_buf=*/batt::ConstBuffer{in_buf, in_bufsz}, out_bufsz,
                                    impl->make_ioctl_handler(req));
}

/**
 * Poll for IO readiness
 *
 * Note: If ph is non-NULL, the client should notify
 * when IO readiness events occur by calling
 * fuse_lowlevel_notify_poll() with the specified ph.
 *
 * Regardless of the number of times poll with a non-NULL ph
 * is received, single notification is enough to clear all.
 * Notifying more times incurs overhead but doesn't harm
 * correctness.
 *
 * The callee is responsible for destroying ph with
 * fuse_pollhandle_destroy() when no longer in use.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as success (with a kernel-defined default poll-mask) and
 * future calls to pull() will succeed the same way without being send
 * to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_poll
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param ph poll handle to be used for notification
 */ // 36/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_poll_impl(fuse_req_t req, fuse_ino_t ino,
                                                       struct fuse_file_info* fi,
                                                       struct fuse_pollhandle* ph)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  (void)ino;
  (void)fi;
  (void)ph;

  LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;
  // TODO [tastolfi 2023-06-28]

  fuse_reply_err(req,
                 FuseImplBase::errno_from_status(batt::Status{batt::StatusCode::kUnimplemented}));
}

/**
 * Write data made available in a buffer
 *
 * This is a more generic version of the ->write() method.  If
 * FUSE_CAP_SPLICE_READ is set in fuse_conn_info.want and the
 * kernel supports splicing from the fuse device, then the
 * data will be made available in pipe for supporting zero
 * copy data transfer.
 *
 * buf->count is guaranteed to be one (and thus buf->idx is
 * always zero). The write_buf handler must ensure that
 * bufv->off is correctly updated (reflecting the number of
 * bytes read from bufv->buf[0]).
 *
 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
 * expected to reset the setuid and setgid bits.
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param bufv buffer containing the data
 * @param off offset to write to
 * @param fi file information
 */ // 37/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_write_buf_impl(fuse_req_t req, fuse_ino_t ino,
                                                            struct fuse_bufvec* bufv, off_t offset,
                                                            struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);
  BATT_CHECK_NOT_NULLPTR(bufv);

  batt::StatusOr<FuseImplBase::ConstBufferVec> buf_vec =
      FuseImplBase::const_buffer_vec_from_bufv(*bufv);

  if (!buf_vec.ok()) {
    fuse_reply_err(req, FuseImplBase::errno_from_status(buf_vec.status()));
    return;
  }

  impl->derived_this()->async_write_buf(req, ino, *buf_vec, FileOffset{offset},
                                        FuseFileHandle{fi->fh}, impl->make_write_handler(req));
}

/**
 * Callback function for the retrieve request
 *
 * Valid replies:
 *	fuse_reply_none
 *
 * @param req request handle
 * @param cookie user data supplied to fuse_lowlevel_notify_retrieve()
 * @param ino the inode number supplied to fuse_lowlevel_notify_retrieve()
 * @param offset the offset supplied to fuse_lowlevel_notify_retrieve()
 * @param bufv the buffer containing the returned data
 */ // 38/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_retrieve_reply_impl(fuse_req_t req, void* cookie,
                                                                 fuse_ino_t ino, off_t offset,
                                                                 struct fuse_bufvec* bufv)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  // TODO [tastolfi 2023-06-28] bufvec -> MutableBufferSequence?
  //
  impl->derived_this()->async_retrieve_reply(req, cookie, ino, FileOffset{offset}, bufv,
                                             impl->make_no_arg_handler(req));
}

/**
 * Forget about multiple inodes
 *
 * See description of the forget function for more
 * information.
 *
 * Valid replies:
 *   fuse_reply_none
 *
 * @param req request handle
 */ // 39/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_forget_multi_impl(fuse_req_t req, size_t count,
                                                               struct fuse_forget_data* forgets)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_forget_multiple_inodes(req, batt::as_slice(forgets, count),
                                                     impl->make_no_arg_handler(req));
}

/**
 * Acquire, modify or release a BSD file lock
 *
 * Note: if the locking methods are not implemented, the kernel
 * will still allow file locking to work locally.  Hence these are
 * only interesting for network filesystems and similar.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param fi file information
 * @param op the locking operation, see flock(2)
 */ // 40/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_flock_impl(fuse_req_t req, fuse_ino_t ino,
                                                        struct fuse_file_info* fi, int op)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  (void)ino;
  (void)fi;
  (void)op;

  LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;
  // TODO [tastolfi 2023-06-28] unpack what kind of request this is and call the appropriate method.

  fuse_reply_err(req,
                 FuseImplBase::errno_from_status(batt::Status{batt::StatusCode::kUnimplemented}));
}

/**
 * Allocate requested space. If this function returns success then
 * subsequent writes to the specified range shall not fail due to the lack
 * of free space on the file system storage media.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future fallocate() requests will fail with EOPNOTSUPP without being
 * send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param offset starting point for allocated region
 * @param length size of allocated region
 * @param mode determines the operation to be performed on the given range,
 *             see fallocate(2)
 */ // 41/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_fallocate_impl(fuse_req_t req, fuse_ino_t ino,
                                                            int mode, off_t offset, off_t length,
                                                            struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  impl->derived_this()->async_file_allocate(req, ino, mode, FileOffset{offset}, FileLength{length},
                                            fi, impl->make_error_handler(req));
}

/**
 * Read directory with attributes
 *
 * Send a buffer filled using fuse_add_direntry_plus(), with size not
 * exceeding the requested size.  Send an empty buffer on end of
 * stream.
 *
 * fi->fh will contain the value set by the opendir method, or
 * will be undefined if the opendir method didn't set any value.
 *
 * In contrast to readdir() (which does not affect the lookup counts),
 * the lookup count of every entry returned by readdirplus(), except "."
 * and "..", is incremented by one.
 *
 * Valid replies:
 *   fuse_reply_buf
 *   fuse_reply_data
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param size maximum number of bytes to send
 * @param off offset to continue reading the directory stream
 * @param fi file information
 */ // 42/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_readdirplus_impl(fuse_req_t req, fuse_ino_t ino,
                                                              size_t size, off_t off,
                                                              struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  BATT_CHECK_NOT_NULLPTR(fi);

  impl->derived_this()->async_readdirplus(req, ino, size, DirentOffset{off}, FuseFileHandle{fi->fh},
                                          impl->make_readdir_handler(req));
}

/**
 * Copy a range of data from one file to another
 *
 * Performs an optimized copy between two file descriptors without the
 * additional cost of transferring data through the FUSE kernel module
 * to user space (glibc) and then back into the FUSE filesystem again.
 *
 * In case this method is not implemented, glibc falls back to reading
 * data from the source and writing to the destination. Effectively
 * doing an inefficient copy of the data.
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EOPNOTSUPP, i.e. all
 * future copy_file_range() requests will fail with EOPNOTSUPP without
 * being send to the filesystem process.
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino_in the inode number or the source file
 * @param off_in starting point from were the data should be read
 * @param fi_in file information of the source file
 * @param ino_out the inode number or the destination file
 * @param off_out starting point where the data should be written
 * @param fi_out file information of the destination file
 * @param len maximum size of the data to copy
 * @param flags passed along with the copy_file_range() syscall
 */ // 43/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_copy_file_range_impl(
    fuse_req_t req, fuse_ino_t ino_in, off_t off_in, struct fuse_file_info* fi_in,
    fuse_ino_t ino_out, off_t off_out, struct fuse_file_info* fi_out, size_t len, int flags)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  (void)ino_in;
  (void)off_in;
  (void)fi_in;

  (void)ino_out;
  (void)off_out;
  (void)fi_out;

  (void)len;
  (void)flags;

  LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;
  // TODO [tastolfi 2023-06-28]

  fuse_reply_err(req,
                 FuseImplBase::errno_from_status(batt::Status{batt::StatusCode::kUnimplemented}));
}

/**
 * Find next data or hole after the specified offset
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure, i.e. all future lseek() requests will
 * fail with the same error code without being send to the filesystem
 * process.
 *
 * Valid replies:
 *   fuse_reply_lseek
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param off offset to start search from
 * @param whence either SEEK_DATA or SEEK_HOLE
 * @param fi file information
 */ // 44/44
template <typename Derived>
/*static*/ inline void FuseImpl<Derived>::op_lseek_impl(fuse_req_t req, fuse_ino_t ino, off_t off,
                                                        int whence, struct fuse_file_info* fi)
{
  void* userdata = fuse_req_userdata(req);
  [[maybe_unused]] auto* impl = static_cast<FuseImpl<Derived>*>(userdata);

  (void)ino;
  (void)off;
  (void)whence;

  BATT_CHECK_NOT_NULLPTR(fi);

  LLFS_LOG_WARNING() << "Not Implemented: " << BATT_THIS_FUNCTION;
  // TODO [tastolfi 2023-06-28]

  fuse_reply_err(req,
                 FuseImplBase::errno_from_status(batt::Status{batt::StatusCode::kUnimplemented}));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename Derived>
/*static*/ inline const fuse_lowlevel_ops* FuseImpl<Derived>::get_fuse_lowlevel_ops()
{
  static const fuse_lowlevel_ops ops_{
      .init = &Self::op_init_impl,                        // 1
      .destroy = &Self::op_destroy_impl,                  // 2
      .lookup = &Self::op_lookup_impl,                    // 3
      .forget = &Self::op_forget_impl,                    // 4
      .getattr = &Self::op_getattr_impl,                  // 5
      .setattr = &Self::op_setattr_impl,                  // 6
      .readlink = &Self::op_readlink_impl,                // 7
      .mknod = &Self::op_mknod_impl,                      // 8
      .mkdir = &Self::op_mkdir_impl,                      // 9
      .unlink = &Self::op_unlink_impl,                    // 10
      .rmdir = &Self::op_rmdir_impl,                      // 11
      .symlink = &Self::op_symlink_impl,                  // 12
      .rename = &Self::op_rename_impl,                    // 13
      .link = &Self::op_link_impl,                        // 14
      .open = &Self::op_open_impl,                        // 15
      .read = &Self::op_read_impl,                        // 16
      .write = &Self::op_write_impl,                      // 17
      .flush = &Self::op_flush_impl,                      // 18
      .release = &Self::op_release_impl,                  // 19
      .fsync = &Self::op_fsync_impl,                      // 20
      .opendir = &Self::op_opendir_impl,                  // 21
      .readdir = &Self::op_readdir_impl,                  // 22
      .releasedir = &Self::op_releasedir_impl,            // 23
      .fsyncdir = &Self::op_fsyncdir_impl,                // 24
      .statfs = &Self::op_statfs_impl,                    // 25
      .setxattr = &Self::op_setxattr_impl,                // 26
      .getxattr = &Self::op_getxattr_impl,                // 27
      .listxattr = &Self::op_listxattr_impl,              // 28
      .removexattr = &Self::op_removexattr_impl,          // 29
      .access = &Self::op_access_impl,                    // 30
      .create = &Self::op_create_impl,                    // 31
      .getlk = &Self::op_getlk_impl,                      // 32
      .setlk = &Self::op_setlk_impl,                      // 33
      .bmap = &Self::op_bmap_impl,                        // 34
      .ioctl = &Self::op_ioctl_impl,                      // 35
      .poll = &Self::op_poll_impl,                        // 36
      .write_buf = &Self::op_write_buf_impl,              // 37
      .retrieve_reply = &Self::op_retrieve_reply_impl,    // 38
      .forget_multi = &Self::op_forget_multi_impl,        // 39
      .flock = &Self::op_flock_impl,                      // 40
      .fallocate = &Self::op_fallocate_impl,              // 41
      .readdirplus = &Self::op_readdirplus_impl,          // 42
      .copy_file_range = &Self::op_copy_file_range_impl,  // 43
      .lseek = &Self::op_lseek_impl,                      // 44
  };

  return &ops_;
}

}  //namespace llfs

#endif  // LLFS_FUSE_IPP
