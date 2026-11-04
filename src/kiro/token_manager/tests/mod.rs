// Copyright (c) 2026 Harllan He. Licensed under MIT.
// token_manager 测试（自 tests.rs 拆出，纯代码搬移）
mod basic;
mod credentials;
mod ext_idp;
mod model_routing;
mod multi;
#[path = "../../../../tests/kiro/opus_model_routing.rs"]
mod opus_model_routing;
mod recovery;
mod refresh;
mod region;
mod sticky;
