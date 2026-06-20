from enum import Enum


class DagId(str, Enum):
    CORE_MAIN = 'core_0_main'
    CORE_TREE_UPDATE = 'core_1_tree_update'
    CORE_METADATA_UPDATE = 'core_2_metadata_update'
    CORE_CAPTION_UPDATE = 'core_3_caption_update'
