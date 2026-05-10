from __future__ import annotations

import hashlib
import json
import logging
import os
from collections.abc import Sequence
from typing import Any
from typing import cast

import pre_commit.constants as C
from pre_commit.all_languages import languages
from pre_commit.clientlib import load_manifest
from pre_commit.clientlib import LOCAL
from pre_commit.clientlib import META
from pre_commit.errors import FatalError
from pre_commit.hook import Hook
from pre_commit.lang_base import environment_dir
from pre_commit.prefix import Prefix
from pre_commit.store import Store
from pre_commit.util import clean_path_on_failure
from pre_commit.util import rmtree


logger = logging.getLogger('pre_commit')


def _state_filename_v1(venv: str) -> str:
    return os.path.join(venv, '.install_state_v1')


def _state_filename_v2(venv: str) -> str:
    return os.path.join(venv, '.install_state_v2')


def _state(hook: Hook) -> object:
    return {
        'additional_dependencies': hook.additional_dependencies,
        'python_lockfile_sha256': hook.python_lockfile_sha256,
    }


def _state_v1_legacy(hook: Hook) -> object:
    return {'additional_dependencies': hook.additional_dependencies}


def _read_state(venv: str) -> object | None:
    filename = _state_filename_v1(venv)
    if not os.path.exists(filename):
        return None
    else:
        with open(filename) as f:
            return json.load(f)


def _hook_installed(hook: Hook) -> bool:
    lang = languages[hook.language]
    if lang.ENVIRONMENT_DIR is None:
        return True

    venv = environment_dir(
        hook.prefix,
        lang.ENVIRONMENT_DIR,
        hook.language_version,
    )
    return (
        (
            os.path.exists(_state_filename_v2(venv)) or
            _read_state(venv) in (_state(hook), _state_v1_legacy(hook))
        ) and
        not lang.health_check(hook.prefix, hook.language_version)
    )


def _hook_install(hook: Hook) -> None:
    logger.info(f'Installing environment for {hook.src}.')
    logger.info('Once installed this environment will be reused.')
    logger.info('This may take a few minutes...')

    lang = languages[hook.language]
    assert lang.ENVIRONMENT_DIR is not None

    venv = environment_dir(
        hook.prefix,
        lang.ENVIRONMENT_DIR,
        hook.language_version,
    )

    # There's potentially incomplete cleanup from previous runs
    # Clean it up!
    if os.path.exists(venv):
        rmtree(venv)

    with clean_path_on_failure(venv):
        if hook.python_lockfile:
            cast(Any, lang).install_environment_locked(
                hook.prefix, hook.language_version, hook.python_lockfile,
            )
        else:
            lang.install_environment(
                hook.prefix, hook.language_version,
                hook.additional_dependencies,
            )
        health_error = lang.health_check(hook.prefix, hook.language_version)
        if health_error:
            raise AssertionError(
                f'BUG: expected environment for {hook.language} to be healthy '
                f'immediately after install, please open an issue describing '
                f'your environment\n\n'
                f'more info:\n\n{health_error}',
            )

        # TODO: remove v1 state writing, no longer needed after pre-commit 3.0
        # Write our state to indicate we're installed
        state_filename = _state_filename_v1(venv)
        staging = f'{state_filename}staging'
        with open(staging, 'w') as state_file:
            state_file.write(json.dumps(_state(hook)))
        # Move the file into place atomically to indicate we've installed
        os.replace(staging, state_filename)

        open(_state_filename_v2(venv), 'a+').close()


def _lockfile_sha256(hook_id: str, lockfile: str) -> str:
    try:
        with open(lockfile, 'rb') as f:
            digest = hashlib.sha256()
            for chunk in iter(lambda: f.read(1024 * 1024), b''):
                digest.update(chunk)
            return digest.hexdigest()
    except OSError as e:
        raise FatalError(
            f'Could not read python_lockfile for hook `{hook_id}`: '
            f'{lockfile}: {e.strerror}',
        )


def _lockfile_path(config_file: str, lockfile: str) -> str:
    if os.path.isabs(lockfile):
        return os.path.normpath(lockfile)
    else:
        return os.path.normpath(
            os.path.join(
                os.path.dirname(os.path.abspath(config_file)),
                lockfile,
            ),
        )


def install_key_for_hook(
        hook: dict[str, Any],
        config_file: str,
) -> tuple[Sequence[str], str]:
    lockfile = hook.get('python_lockfile', '')
    if not lockfile:
        return hook.get('additional_dependencies', ()), ''

    lockfile = _lockfile_path(config_file, lockfile)
    return (), _lockfile_sha256(hook['id'], lockfile)


def _hook(
        *hook_dicts: dict[str, Any],
        root_config: dict[str, Any],
        config_file: str,
) -> dict[str, Any]:
    ret, rest = dict(hook_dicts[0]), hook_dicts[1:]
    for dct in rest:
        ret.update(dct)

    lang = ret['language']
    if ret['language_version'] == C.DEFAULT:
        ret['language_version'] = root_config['default_language_version'][lang]
    if ret['language_version'] == C.DEFAULT:
        ret['language_version'] = languages[lang].get_default_version()

    if not ret['stages']:
        ret['stages'] = root_config['default_stages']

    ret.setdefault('python_lockfile', '')
    ret['python_lockfile_sha256'] = ''
    if ret['python_lockfile']:
        if lang != 'python':
            logger.error(
                f'The hook `{ret["id"]}` specifies `python_lockfile` but is '
                f'using language `{lang}`.  `python_lockfile` is only '
                f'supported for language `python`.',
            )
            exit(1)
        if ret['additional_dependencies']:
            logger.error(
                f'The hook `{ret["id"]}` specifies `python_lockfile` and '
                f'`additional_dependencies`.  These options are mutually '
                f'exclusive.',
            )
            exit(1)

        ret['python_lockfile'] = _lockfile_path(
            config_file, ret['python_lockfile'],
        )
        ret['python_lockfile_sha256'] = _lockfile_sha256(
            ret['id'], ret['python_lockfile'],
        )

    if languages[lang].ENVIRONMENT_DIR is None:
        if ret['language_version'] != C.DEFAULT:
            logger.error(
                f'The hook `{ret["id"]}` specifies `language_version` but is '
                f'using language `{lang}` which does not install an '
                f'environment.  '
                f'Perhaps you meant to use a specific language?',
            )
            exit(1)
        if ret['additional_dependencies']:
            logger.error(
                f'The hook `{ret["id"]}` specifies `additional_dependencies` '
                f'but is using language `{lang}` which does not install an '
                f'environment.  '
                f'Perhaps you meant to use a specific language?',
            )
            exit(1)

    return ret


def _non_cloned_repository_hooks(
        repo_config: dict[str, Any],
        store: Store,
        root_config: dict[str, Any],
        config_file: str,
) -> tuple[Hook, ...]:
    def _prefix(
            language_name: str,
            deps: Sequence[str],
            python_lockfile_sha256: str,
    ) -> Prefix:
        language = languages[language_name]
        # pygrep / script / system / docker_image do not have
        # environments so they work out of the current directory
        if language.ENVIRONMENT_DIR is None:
            return Prefix(os.getcwd())
        else:
            return Prefix(store.make_local(deps, python_lockfile_sha256))

    ret = []
    for hook in repo_config['hooks']:
        hook = _hook(hook, root_config=root_config, config_file=config_file)
        ret.append(
            Hook.create(
                repo_config['repo'],
                _prefix(
                    hook['language'], hook['additional_dependencies'],
                    hook['python_lockfile_sha256'],
                ),
                hook,
            ),
        )
    return tuple(ret)


def _cloned_repository_hooks(
        repo_config: dict[str, Any],
        store: Store,
        root_config: dict[str, Any],
        config_file: str,
) -> tuple[Hook, ...]:
    repo, rev = repo_config['repo'], repo_config['rev']
    manifest_path = os.path.join(store.clone(repo, rev), C.MANIFEST_FILE)
    by_id = {hook['id']: hook for hook in load_manifest(manifest_path)}

    for hook in repo_config['hooks']:
        if hook['id'] not in by_id:
            logger.error(
                f'`{hook["id"]}` is not present in repository {repo}.  '
                f'Typo? Perhaps it is introduced in a newer version?  '
                f'Often `pre-commit autoupdate` fixes this.',
            )
            exit(1)

    hook_dcts = [
        _hook(
            by_id[hook['id']], hook, root_config=root_config,
            config_file=config_file,
        )
        for hook in repo_config['hooks']
    ]
    return tuple(
        Hook.create(
            repo_config['repo'],
            Prefix(
                store.clone(
                    repo, rev, hook['additional_dependencies'],
                    hook['python_lockfile_sha256'],
                ),
            ),
            hook,
        )
        for hook in hook_dcts
    )


def _repository_hooks(
        repo_config: dict[str, Any],
        store: Store,
        root_config: dict[str, Any],
        config_file: str,
) -> tuple[Hook, ...]:
    if repo_config['repo'] in {LOCAL, META}:
        return _non_cloned_repository_hooks(
            repo_config, store, root_config, config_file,
        )
    else:
        return _cloned_repository_hooks(
            repo_config, store, root_config, config_file,
        )


def install_hook_envs(hooks: Sequence[Hook], store: Store) -> None:
    def _need_installed() -> list[Hook]:
        seen: set[tuple[Prefix, str, str, tuple[str, ...], str]] = set()
        ret = []
        for hook in hooks:
            if hook.install_key not in seen and not _hook_installed(hook):
                ret.append(hook)
            seen.add(hook.install_key)
        return ret

    if not _need_installed():
        return
    with store.exclusive_lock():
        # Another process may have already completed this work
        for hook in _need_installed():
            _hook_install(hook)


def all_hooks(
        root_config: dict[str, Any],
        store: Store,
        config_file: str = C.CONFIG_FILE,
) -> tuple[Hook, ...]:
    return tuple(
        hook
        for repo in root_config['repos']
        for hook in _repository_hooks(repo, store, root_config, config_file)
    )
