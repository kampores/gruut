"""Class for getting phonetic pronunciations for tokenized text"""
import itertools
import logging
import sqlite3
import typing
from pathlib import Path

from gruut.const import PHONEMES_TYPE
import espeak_phonemizer

# -----------------------------------------------------------------------------

_LOGGER = logging.getLogger("gruut.phonemize")

ROLE_TO_PHONEMES = typing.Dict[str, PHONEMES_TYPE]

WORD_TRANSFORM_TYPE = typing.Callable[[str], str]


# -----------------------------------------------------------------------------


class SqlitePhonemizer:
    """Phonemizes text using a lexicon from a sqlite database"""

    DEFAULT_ROLE: str = ""

    def __init__(
        self,
        lang:str,
        db_update_on: bool,
        db_conn: sqlite3.Connection,
        lexicon: typing.Optional[typing.Dict[str, ROLE_TO_PHONEMES]] = None,
        g2p_model: typing.Optional[typing.Dict[str, typing.Union[str, Path]]] = None,
        word_transform_funcs: typing.Optional[
            typing.Iterable[WORD_TRANSFORM_TYPE]
        ] = None,
        casing_func: typing.Optional[WORD_TRANSFORM_TYPE] = None,
    ):
        self.db_conn = db_conn

        # word -> role -> [phonemes]
        self.lexicon = lexicon if lexicon is not None else {}

        # [functions]
        self.word_transform_funcs = word_transform_funcs or []

        self.casing_func = casing_func

        self.espeak_phonemizer = espeak_phonemizer.Phonemizer(default_voice=lang)

        self.db_update_on = db_update_on

    def __call__(
        self, word: str, role: typing.Optional[str] = None, do_transforms: bool = True
    ) -> typing.Optional[PHONEMES_TYPE]:
        # Look up in cache first
        if self.casing_func is not None:
            word = self.casing_func(word)

        role_to_word = self.lexicon.get(word)

        if role_to_word is not None:
            if role is not None:
                # Exact role
                phonemes = role_to_word.get(role)
                if phonemes is not None:
                    return phonemes

            # Default role
            phonemes = role_to_word.get(SqlitePhonemizer.DEFAULT_ROLE)
            if phonemes is not None:
                return phonemes

            # Any role
            if role_to_word:
                return next(iter(role_to_word.values()))

            # Not in lexicon (or database) for sure because role_to_word was present.
            return None

        transforms = self.word_transform_funcs
        if not do_transforms:
            # No transforms
            transforms = []

        for transform_func in itertools.chain([None], transforms):
            if transform_func is not None:
                lookup_word = transform_func(word)
            else:
                # No transform
                lookup_word = word

            if not lookup_word:
                continue

            # Load pronunciations for word from database.
            if not role:
                cursor = self.db_conn.execute(
                    "SELECT role, phonemes FROM word_phonemes WHERE word = ? ORDER BY pron_order",
                    (lookup_word,),
                )
            else:
                cursor = self.db_conn.execute(
                    "SELECT role, phonemes FROM word_phonemes WHERE word = ? AND role = ? ORDER BY pron_order",
                    (lookup_word, role),
                )
            rows = cursor.fetchall()
            if not rows :
                phonemes = self.espeak_phonemizer.phonemize(lookup_word, keep_clause_breakers=False)
                if self.db_update_on:
                    pron_order = 0
                    try:
                        cursor = self.db_conn.execute(
                            "INSERT INTO word_phonemes VALUES ((SELECT IFNULL(MAX(id), 0) + 1 FROM word_phonemes), ?, (SELECT IFNULL(MAX(pron_order) + 1, 0) FROM word_phonemes WHERE word = ?), ?, ?)",
                            (lookup_word, lookup_word, phonemes, role),
                        )
                        self.db_conn.commit()
                    except:
                        print(lookup_word, pron_order, phonemes, role)
                        # raise

            # for row in cursor:
            for row in rows:
                if role_to_word is None:
                    # Create new lexicon entry for original word
                    role_to_word = {}
                    self.lexicon[word] = role_to_word

                db_role, db_phonemes = row[0], row[1].split()

                if db_role not in role_to_word:
                    role_to_word[db_role] = db_phonemes

            if role_to_word is not None:
                # Link to transformed word
                self.lexicon[lookup_word] = self.lexicon[word]

                # Successfully looked up in the database
                return self(word, role=role)

        # Not in lexicon
        return None
