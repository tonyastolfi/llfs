#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
#
# Part of the LLFS Project, under Apache License v2.0.
# See https://www.apache.org/licenses/LICENSE-2.0 for license information.
# SPDX short identifier: Apache-2.0
#
#+++++++++++-+-+--+----- --- -- -  -  -   -

from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps
from conan.tools.files import copy

import os, sys, platform


#==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
# Import batt helper utilities module.
#
sys.path.append(os.path.join(os.path.dirname(__file__), 'script'))
import batt
#
#+++++++++++-+-+--+----- --- -- -  -  -   -


class LlfsConan(ConanFile):
    name = "llfs"
    # version is set automatically from Git tags - DO NOT SET IT HERE
    license = "Apache Public License 2.0"
    author = "The MathWorks, Inc."
    url = "https://github.com/mathworks/llfs"
    description = "Low-Level File System Utilities (C++)"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False]}
    default_options = {"shared": False}
    build_policy = "missing"

    exports = [
        "script/*.py",
        "script/*.sh",
    ]
    exports_sources = [
        "src/CMakeLists.txt",
        "src/**/*.hpp",
        "src/**/*.ipp",
        "src/**/*.cpp",
    ]
            
    #+++++++++++-+-+--+----- --- -- -  -  -   -

    def set_version(self):
        self.version = batt.get_version(no_check_conan=True)
        batt.verbose(f'VERSION={self.version}')


    def requirements(self):
        deps = [
            "batteries/0.49.1",
            "boost/1.83.0",
            "cli11/2.3.2",
            "glog/0.6.0",
            "gtest/1.14.0",
            "libbacktrace/cci.20210118",
            "openssl/3.1.3",
        ]

        override_deps = [
            "zlib/1.2.13",
        ]

        platform_deps = {
            "Linux": [
                "liburing/2.4",
                "libfuse/3.10.5",
            ]
        }

        if platform.system() == 'Linux':
            self.requires("libunwind/1.7.2",
                          override=True,
                          visible=True,
                          transitive_headers=True,
                          transitive_libs=True)

        batt.conanfile_requirements(self, deps, override_deps, platform_deps)


    def layout(self):
        cmake_layout(self, src_folder="src")


    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()
        batt.generate_conan_find_requirements(self)


    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["boost"].without_test = True
        self.options["batteries"].with_glog = True
        self.options["batteries"].header_only = False


    def build(self):
        cmake = CMake(self)
        cmake.verbose = batt.VERBOSE
        cmake.configure()
        cmake.build()


    def package(self):
        src_include = os.path.join(self.source_folder, ".")
        dst_include = os.path.join(self.package_folder, "include")

        copy(self, "*.hpp", dst=dst_include, src=src_include)
        copy(self, "*.ipp", dst=dst_include, src=src_include)

        cmake = CMake(self)
        cmake.configure()
        cmake.install()


    def package_info(self):
        self.cpp_info.cxxflags = ["-D_GNU_SOURCE", "-D_BITS_UIO_EXT_H=1"]
        self.cpp_info.system_libs = ["dl"]
        self.cpp_info.libs = ["llfs"]
