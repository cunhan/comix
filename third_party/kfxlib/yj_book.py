import io
import os
import posixpath
import traceback

from .ion_symbol_table import (LocalSymbolTable, SymbolTableCatalog)
from .ion_binary import IonBinary
from .ion_text import IonText
from .kfx_container import (KfxContainer, MAX_KFX_CONTAINER_SIZE)
from .kpf_book import KpfBook
from .kpf_container import KpfContainer
from .message_logging import log
from .unpack_container import (IonTextContainer, JsonContentContainer, ZipUnpackContainer)
from .utilities import (
        DataFile, file_read_utf8, flush_unicode_cache, bytes_to_separated_hex,
        KFXDRMError, make_progress, temp_file_cleanup, ZIP_SIGNATURE)
from .yj_container import YJFragmentList
from .yj_metadata import BookMetadata
from .yj_position_location import BookPosLoc
from .yj_structure import BookStructure
from .yj_symbol_catalog import (IonSharedSymbolTable, YJ_SYMBOLS)


__license__ = "GPL v3"
__copyright__ = "2016-2025, John Howell <jhowell@acm.org>"


class YJ_Book(BookStructure, BookPosLoc, BookMetadata, KpfBook):
    def __init__(self, file, credentials=[], is_netfs=False, symbol_catalog_filename=None):
        self.datafile = DataFile(file)
        self.credentials = credentials
        self.is_netfs = is_netfs
        self.symbol_catalog_filename = symbol_catalog_filename
        self.reported_errors = set()
        self.symtab = LocalSymbolTable(YJ_SYMBOLS.name)
        self.fragments = YJFragmentList()
        self.reported_missing_fids = set()
        self.is_kpf_prepub = self.is_dictionary = self.is_scribe_notebook = False
        self.is_entity_dependencies_modified = False
        self.yj_containers = []
        self.kpf_container = None

        self.load_symbol_catalog()

    def load_symbol_catalog(self):
        if self.symbol_catalog_filename is not None:
            if not os.path.isfile(self.symbol_catalog_filename):
                raise Exception("Symbol catalog %s does not exist" % self.symbol_catalog_filename)

            translation_catalog = SymbolTableCatalog()
            catalog_symtab = LocalSymbolTable(catalog=translation_catalog)

            try:
                IonText(catalog_symtab).deserialize_multiple_values(
                        file_read_utf8(self.symbol_catalog_filename), import_symbols=True)
            except Exception:
                log.error("Failed to parse symbol catalog %s" % self.symbol_catalog_filename)
                raise

            translation_symtab = translation_catalog.get_shared_symbol_table(YJ_SYMBOLS.name)
            if translation_symtab is None:
                raise Exception("Symbol catalog %s does not contain a definition for YJ_symbols" % self.symbol_catalog_filename)

            catalog_symtab.report()
            log.info("Symbol catalog defines %d symbols in YJ_symbols" % len(translation_symtab.symbols))
        else:
            translation_symtab = IonSharedSymbolTable(YJ_SYMBOLS.name)

        self.symtab.set_translation(translation_symtab)

    def final_actions(self, do_symtab_report=True):
        if do_symtab_report:
            self.symtab.report()

        flush_unicode_cache()
        temp_file_cleanup()

    def convert_to_single_kfx(self):
        self.decode_book()

        if self.is_dictionary:
            log.error("Cannot serialize a dictionary as a KFX container")
            return None

        if self.is_scribe_notebook:
            log.error("Cannot serialize a Scribe notebook as a KFX container")
            return None

        if self.is_kpf_prepub:
            log.error("Cannot serialize KPF as a KFX container without fix-up")
            return None

        result = KfxContainer(self.symtab, fragments=self.fragments).serialize()

        if len(result) > MAX_KFX_CONTAINER_SIZE:
            log.warning("KFX container created may be too large for some devices (%d bytes)" % len(result))
            pass

        self.final_actions()
        return result

    def convert_to_epub(self, epub2_desired=False, force_cover=False, progress_fn=None):
        from .yj_to_epub import KFX_EPUB
        self.decode_book()
        result = KFX_EPUB(self, epub2_desired=epub2_desired, force_cover=force_cover,
                          progress=make_progress(progress_fn)).decompile_to_epub()
        self.final_actions()
        return result

    def convert_to_cbz(self, split_landscape_comic_images=False, progress_fn=None):
        from .yj_to_image_book import KFX_IMAGE_BOOK
        self.decode_book()
        result = KFX_IMAGE_BOOK(self).convert_book_to_cbz(split_landscape_comic_images, make_progress(progress_fn))
        self.final_actions()
        return result

    def convert_to_pdf(self, split_landscape_comic_images=False, progress_fn=None):
        from .yj_to_image_book import KFX_IMAGE_BOOK
        self.decode_book()
        result = KFX_IMAGE_BOOK(self).convert_book_to_pdf(split_landscape_comic_images, make_progress(progress_fn))
        self.final_actions()
        return result

    def get_metadata(self):

        self.locate_book_datafiles()

        yj_datafile_containers = []
        for datafile in self.container_datafiles:
            try:
                container = self.get_container(datafile, ignore_drm=True)
                if container is not None:
                    container.deserialize(ignore_drm=True)
                    yj_datafile_containers.append((datafile, container))

            except Exception as e:
                log.warning("Failed to extract content from %s: %s" % (datafile.name, repr(e)))

        for datafile, container in yj_datafile_containers:
            try:
                self.fragments.extend(container.get_fragments())

            except Exception as e:
                log.warning("Failed to extract content from %s: %s" % (datafile.name, repr(e)))
                continue

            if self.has_metadata() and self.has_cover_data():
                break

        if not self.has_metadata():
            raise Exception("Failed to locate a KFX container with metadata")

        self.final_actions(do_symtab_report=False)
        return self.get_yj_metadata_from_book()

    def convert_to_kpf(self, conversion=None, flags=None, timeout_sec=None, cleaned_filename=None):
        from .generate_kpf_common import ConversionResult
        from .generate_kpf_using_cli import KPR_CLI

        if not self.datafile.is_real_file:
            raise Exception("Cannot create KPF from stream")

        infile = self.datafile.name
        intype = os.path.splitext(infile)[1].lower()

        if not conversion:
            conversion = "KPR_CLI"

        flags = set() if flags is None else set(flags)

        options = conversion.split("/")
        conversion_name = options[0]
        flags |= set(options[1:])

        ALL_TYPES = [".doc", ".docx", ".epub", ".mobi", ".opf"]

        if conversion_name == "KPR_CLI" and intype in ALL_TYPES:
            conversion_sequence = KPR_CLI()
        else:
            return ConversionResult(error_msg="Cannot generate KPF from %s file using %s" % (intype, conversion_name))

        try:
            result = conversion_sequence.convert_to_kpf(infile, flags, timeout_sec, cleaned_filename)
        except Exception as e:
            traceback.print_exc()
            result = ConversionResult(error_msg=repr(e))

        self.final_actions(do_symtab_report=False)
        return result

    def convert_to_zip_unpack(self):
        self.decode_book()
        result = ZipUnpackContainer(self.symtab, fragments=self.fragments).serialize()
        self.final_actions()
        return result

    def convert_to_json_content(self, keep_footnote_refs=False):
        self.decode_book()
        result = JsonContentContainer(self).serialize(keep_footnote_refs)
        self.final_actions()
        return result

    def decode_book(self, set_metadata=None, set_approximate_pages=None, pure=False, retain_yj_locals=False):
        if self.fragments:
            if set_metadata is not None or set_approximate_pages is not None or retain_yj_locals:
                raise Exception("Attempt to change metadata after book has already been decoded")
            return

        self.locate_book_datafiles()

        for datafile in self.container_datafiles:
            log.info("Processing container: %s" % datafile.name)
            container = self.get_container(datafile)
            container.deserialize()
            self.yj_containers.append(container)

        for container in self.yj_containers:
            self.fragments.extend(container.get_fragments())

        if self.is_kpf_prepub:
            self.fix_kpf_prepub_book(not pure, retain_yj_locals)

        if True:
            self.check_consistency()

        if not pure:
            if set_metadata is not None:
                self.set_yj_metadata_to_book(set_metadata)

            if set_approximate_pages is not None and set_approximate_pages >= 0:
                try:
                    self.create_approximate_page_list(set_approximate_pages)
                except Exception as e:
                    traceback.print_exc()
                    log.error("Exception creating approximate page numbers: %s" % repr(e))

        try:
            self.report_features_and_metadata(unknown_only=False)
        except Exception as e:
            traceback.print_exc()
            log.error("Exception checking book features and metadata: %s" % repr(e))

        self.check_fragment_usage(rebuild=not pure, ignore_extra=False)
        self.check_symbol_table(rebuild=not pure, ignore_unused=self.is_scribe_notebook)

        self.final_actions()

    def locate_book_datafiles(self):
        self.container_datafiles = []

        if self.datafile.is_real_file and os.path.isdir(self.datafile.name):
            self.locate_files_from_dir(self.datafile.name)

        elif self.datafile.ext in [".azw8", ".ion", ".kfx", ".kpf"]:

            self.container_datafiles.append(self.datafile)

            if self.datafile.ext == ".kfx" and self.datafile.is_real_file:
                sdr_dirname = os.path.splitext(self.datafile.name)[0] + ".sdr"
                if os.path.isdir(sdr_dirname):
                    self.locate_files_from_dir(sdr_dirname)

        elif self.datafile.ext in [".kfx-zip", ".zip"]:
            with self.datafile.as_ZipFile() as zf:
                for info in zf.infolist():
                    if posixpath.basename(info.filename).lower() in ["book.ion", "book.kdf"]:
                        self.container_datafiles.append(self.datafile)
                        break
                else:
                    for info in zf.infolist():
                        self.check_located_file(info.filename, zf.read(info), self.datafile)

        else:
            raise Exception("Unknown main file type %s. Must be azw8, ion, kfx, kfx-zip, kpf, or zip." % self.datafile.ext)

        if not self.container_datafiles:
            raise Exception("No KFX containers found. This book is not in KFX format.")

        self.container_datafiles = sorted(self.container_datafiles)

    def locate_files_from_dir(self, directory, match=None):
        for dirpath, dirnames, filenames in os.walk(directory):
            for fn in filenames:
                if (not match) or match == fn:
                    self.check_located_file(os.path.join(dirpath, fn))

    def check_located_file(self, name, data=None, parent=None):
        basename = posixpath.basename(name.replace("\\", "/")).lower()
        ext = os.path.splitext(basename)[1]

        if basename.startswith("._") or basename == "BookManifest.kfx":
            pass
        elif ext in [".azw", ".azw8", ".azw9", ".kfx", ".md", ".res", ".yj"] or basename == "nbk":
            self.container_datafiles.append(DataFile(name, data, parent))
        elif basename == "nbk-journal":
            datafile = DataFile(name, data, parent)
            if len(datafile.get_data()) > 0:
                log.warning("nbk-journal is not empty")

    def get_container(self, datafile, ignore_drm=False):
        data = datafile.get_data()

        if datafile.ext == ".ion" and not data.startswith(IonBinary.SIGNATURE):
            return IonTextContainer(self.symtab, datafile)

        if data.startswith(ZIP_SIGNATURE):
            with datafile.as_ZipFile() as zf:
                for info in zf.infolist():
                    if posixpath.basename(info.filename).lower() in ["book.ion", "book.kdf"]:
                        if info.filename.lower().endswith(".kdf"):
                            return KpfContainer(self.symtab, datafile, book=self)
                        else:
                            return ZipUnpackContainer(self.symtab, datafile)

        if data.startswith(KpfContainer.KDF_SIGNATURE):
            return KpfContainer(self.symtab, datafile, book=self)

        if data.startswith(KfxContainer.SIGNATURE):
            return KfxContainer(self.symtab, datafile)

        if data.startswith(KfxContainer.DRM_SIGNATURE):

            if datafile.name.endswith("metadata.kfx"):
                expanded_data = self.expand_compressed_container(data)
                if expanded_data:
                    return self.get_container(DataFile(datafile.name + ".decompressed", expanded_data))

            if ignore_drm:
                return None

            raise KFXDRMError("Book container %s has DRM and cannot be converted" % datafile.name)

        if data[0x3c:0x3c+8] in [b"BOOKMOBI", b"RBINCONT"]:
            raise Exception("File format is MOBI (not KFX) for %s" % datafile.name)

        raise Exception("Unable to determine KFX container type of %s (%s)" % (datafile.name, bytes_to_separated_hex(data[:8])))

    def expand_compressed_container(self, data):
        try:
            from calibre_plugins.dedrm.ion import DrmIon
            outfile = io.BytesIO()
            DrmIon(io.BytesIO(data[8:-8]), None).parse(outfile)
        except Exception:
            expanded_data = None
        else:
            expanded_data = outfile.getvalue()

        return expanded_data if expanded_data and expanded_data.startswith(KfxContainer.SIGNATURE) else None
